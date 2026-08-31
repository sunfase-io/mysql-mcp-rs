use crate::{
    db::{ConnectionConfig, DbIdentity, ReconnectNotice, Session, SessionManager, kill_query},
    files::{AtomicTarget, export_path, validate_export_paths},
    objects::{ObjectRef, find_ddl, normalize_type, show_create_sql},
    sql::{self, StatementKind},
    values::{bind_params, column_names, row_to_json},
};
use mysql_async::prelude::Queryable;
use rmcp::{
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

const DEFAULT_MAX_ROWS: usize = 200;
const DEFAULT_MAX_BYTES: usize = 32 * 1024;
static SAVEPOINT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct MysqlMcp {
    sessions: Arc<SessionManager>,
    tool_router: ToolRouter<Self>,
}

impl MysqlMcp {
    pub fn new() -> Self {
        let sessions = Arc::new(SessionManager::default());
        sessions.start_idle_reaper();
        Self {
            sessions,
            tool_router: Self::tool_router(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ConnectArgs {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub connection_id: Option<String>,
}

fn default_port() -> u16 {
    3306
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ConnectionArgs {
    pub connection_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RequiredConnectionArgs {
    pub connection_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct QueryArgs {
    pub sql: String,
    pub params: Option<Vec<Value>>,
    pub max_rows: Option<usize>,
    pub max_bytes: Option<usize>,
    pub timeout_secs: Option<u64>,
    pub connection_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExecuteArgs {
    pub sql: String,
    pub params: Option<Vec<Value>>,
    pub timeout_secs: Option<u64>,
    pub connection_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExecuteManyArgs {
    pub sql: String,
    pub params_list: Vec<Vec<Value>>,
    pub timeout_secs: Option<u64>,
    pub connection_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct QueryToFileArgs {
    pub sql: String,
    pub params: Option<Vec<Value>>,
    pub path: String,
    #[serde(default = "default_file_format")]
    pub format: String,
    #[serde(default)]
    pub overwrite: bool,
    pub timeout_secs: Option<u64>,
    pub connection_id: Option<String>,
}

fn default_file_format() -> String {
    "jsonl".to_owned()
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListTablesArgs {
    pub database: Option<String>,
    pub name_like: Option<String>,
    pub max_rows: Option<usize>,
    pub connection_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DescribeTableArgs {
    pub database: String,
    pub table: String,
    pub connection_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListObjectsArgs {
    pub database: Option<String>,
    #[serde(rename = "type")]
    pub object_type: Option<String>,
    pub name_like: Option<String>,
    pub max_rows: Option<usize>,
    pub connection_id: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ObjectArgs {
    pub database: String,
    #[serde(rename = "type")]
    pub object_type: String,
    pub name: String,
    pub connection_id: Option<String>,
}

impl From<&ObjectArgs> for ObjectRef {
    fn from(value: &ObjectArgs) -> Self {
        Self {
            database: value.database.clone(),
            object_type: value.object_type.clone(),
            name: value.name.clone(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ExportObjectsArgs {
    pub objects: Vec<ObjectRef>,
    pub out_dir: String,
    #[serde(default)]
    pub overwrite: bool,
    pub connection_id: Option<String>,
}

fn tool_error(error: impl std::fmt::Display) -> CallToolResult {
    CallToolResult::structured_error(json!({
        "ok": false,
        "error": error.to_string(),
        "_db": null,
    }))
}

fn tool_ok(value: Value) -> CallToolResult {
    CallToolResult::structured(value)
}

fn add_db(mut body: Value, identity: &DbIdentity, notice: ReconnectNotice) -> Value {
    body["_db"] = serde_json::to_value(identity).expect("DbIdentity 必须可序列化");
    if notice.reconnected {
        body["_db"]["reconnected"] = json!(true);
    }
    if notice.transaction_lost {
        body["_db"]["transaction_lost"] = json!(true);
    }
    body
}

struct RunningGuard<'a>(&'a AtomicBool);

impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

async fn cancellable<T>(
    session: &Session,
    timeout_secs: Option<u64>,
    operation: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    anyhow::ensure!(
        !session.running.swap(true, Ordering::AcqRel),
        "连接 `{}` 已有语句正在执行",
        session.id
    );
    session.cancel_requested.store(false, Ordering::Release);
    let _guard = RunningGuard(&session.running);
    let seconds = timeout_secs.unwrap_or(0);
    if seconds == 0 {
        let result = operation.await;
        mark_unusable_on_fatal(session, &result);
        if session.cancel_requested.swap(false, Ordering::AcqRel) {
            return Err(anyhow::anyhow!("语句已由 cancel_statement 取消"));
        }
        return result;
    }

    tokio::pin!(operation);
    tokio::select! {
        result = &mut operation => {
            mark_unusable_on_fatal(session, &result);
            if session.cancel_requested.swap(false, Ordering::AcqRel) {
                Err(anyhow::anyhow!("语句已由 cancel_statement 取消"))
            } else {
                result
            }
        },
        _ = tokio::time::sleep(std::time::Duration::from_secs(seconds)) => {
            let thread_id = session.thread_id.load(Ordering::Acquire);
            let cancellation = kill_query(&session.config, thread_id).await;
            if let Err(error) = cancellation {
                session.unusable.store(true, Ordering::Release);
                return Err(anyhow::anyhow!(
                    "语句执行超过 {seconds} 秒，KILL QUERY 失败；连接已标记为不可复用: {error}"
                ));
            }
            let tail = operation.await;
            mark_unusable_on_fatal(session, &tail);
            Err(anyhow::anyhow!("语句执行超过 {seconds} 秒，已通过 KILL QUERY 取消"))
        }
    }
}

fn mark_unusable_on_fatal<T>(session: &Session, result: &anyhow::Result<T>) {
    if result
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<mysql_async::Error>())
        .is_some_and(mysql_async::Error::is_fatal)
    {
        session.unusable.store(true, Ordering::Release);
    }
}

struct QueryData {
    columns: Vec<String>,
    rows: Vec<Value>,
    returned_bytes: usize,
    total_rows: u64,
    truncation_reason: Option<&'static str>,
}

impl MysqlMcp {
    async fn run_query(&self, args: QueryArgs, enforce_read: bool) -> anyhow::Result<Value> {
        if enforce_read {
            sql::require_read(&args.sql)?;
        }
        let params = bind_params(args.params)?;
        let max_rows = args.max_rows.unwrap_or(DEFAULT_MAX_ROWS);
        let max_bytes = args.max_bytes.unwrap_or(DEFAULT_MAX_BYTES);
        let session = self.sessions.get(args.connection_id.as_deref()).await?;
        let mut state = session.state.lock().await;
        let notice = session.reconnect_if_needed(&mut state).await?;
        let identity = state.identity.clone();
        let conn = state.conn.as_mut().expect("重连后必须存在连接");
        let sql = args.sql;

        let data = cancellable(&session, args.timeout_secs, async {
            let mut result = conn.exec_iter(sql, params).await?;
            let columns = result.columns().unwrap_or_default();
            let names = column_names(&columns)?;
            let mut rows = Vec::new();
            let mut returned_bytes = 0;
            let mut total_rows = 0_u64;
            let mut truncation_reason = None;
            while let Some(row) = result.next().await? {
                total_rows = total_rows
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("查询总行数超出 u64"))?;
                if truncation_reason.is_some() {
                    continue;
                }
                if let Some(reason) =
                    limit_reason(rows.len(), returned_bytes, 0, max_rows, max_bytes)
                {
                    truncation_reason = Some(reason);
                    continue;
                }
                let row = row_to_json(row, &columns, &names)?;
                let bytes = serde_json::to_vec(&row)?.len();
                if let Some(reason) =
                    limit_reason(rows.len(), returned_bytes, bytes, max_rows, max_bytes)
                {
                    truncation_reason = Some(reason);
                    continue;
                }
                returned_bytes += bytes;
                rows.push(row);
            }
            result.drop_result().await?;
            Ok(QueryData {
                columns: names,
                rows,
                returned_bytes,
                total_rows,
                truncation_reason,
            })
        })
        .await?;
        state.last_used = Instant::now();

        let returned_rows = data.rows.len();
        Ok(add_db(
            json!({
                "columns": data.columns,
                "rows": data.rows,
                "row_count": returned_rows.to_string(),
                "total_rows": data.total_rows.to_string(),
                "returned_bytes": data.returned_bytes.to_string(),
                "truncated": data.truncation_reason.is_some(),
                "truncation_reason": data.truncation_reason,
            }),
            &identity,
            notice,
        ))
    }

    async fn fetch_ddl(
        &self,
        object: &ObjectRef,
        connection_id: Option<String>,
    ) -> anyhow::Result<(String, DbIdentity, ReconnectNotice)> {
        let sql = show_create_sql(object)?;
        let body = self
            .run_query(
                QueryArgs {
                    sql,
                    params: None,
                    max_rows: Some(1),
                    max_bytes: Some(16 * 1024 * 1024),
                    timeout_secs: None,
                    connection_id,
                },
                true,
            )
            .await?;
        let row = body["rows"]
            .as_array()
            .and_then(|rows| rows.first())
            .ok_or_else(|| anyhow::anyhow!("对象不存在或 SHOW CREATE 未返回结果"))?;
        let ddl = find_ddl(row)?;
        let notice = ReconnectNotice {
            reconnected: body["_db"]["reconnected"].as_bool().unwrap_or(false),
            transaction_lost: body["_db"]["transaction_lost"].as_bool().unwrap_or(false),
        };
        let identity: DbIdentity = serde_json::from_value(body["_db"].clone())?;
        Ok((ddl, identity, notice))
    }
}

fn limit_reason(
    returned_rows: usize,
    returned_bytes: usize,
    next_row_bytes: usize,
    max_rows: usize,
    max_bytes: usize,
) -> Option<&'static str> {
    if returned_rows >= max_rows {
        Some("max_rows")
    } else if returned_bytes
        .checked_add(next_row_bytes)
        .is_none_or(|total| total > max_bytes)
    {
        Some("max_bytes")
    } else {
        None
    }
}

#[tool_router]
impl MysqlMcp {
    #[tool(description = "连接 MySQL；密码仅保存在进程内存中，重复 connection_id 会失败")]
    async fn connect(&self, Parameters(args): Parameters<ConnectArgs>) -> CallToolResult {
        let result = async {
            anyhow::ensure!(!args.host.trim().is_empty(), "host 不能为空");
            anyhow::ensure!(args.port > 0, "port 必须大于 0");
            anyhow::ensure!(!args.user.trim().is_empty(), "user 不能为空");
            anyhow::ensure!(!args.database.trim().is_empty(), "database 不能为空");
            let id = args
                .connection_id
                .unwrap_or_else(|| format!("{}:{}/{}", args.host, args.port, args.database));
            let session = self
                .sessions
                .connect(
                    id,
                    ConnectionConfig {
                        host: args.host,
                        port: args.port,
                        user: args.user,
                        password: args.password,
                        database: args.database,
                    },
                )
                .await?;
            let state = session.state.lock().await;
            Ok::<_, anyhow::Error>(add_db(
                json!({"connected": true}),
                &state.identity,
                ReconnectNotice::default(),
            ))
        }
        .await;
        result.map(tool_ok).unwrap_or_else(tool_error)
    }

    #[tool(description = "列出进程内的 MySQL 连接及事务状态")]
    async fn list_connections(&self) -> CallToolResult {
        let result = async {
            let active = self.sessions.active_id().await;
            let mut connections = Vec::new();
            let mut active_identity = None;
            for session in self.sessions.list().await {
                let state = session.state.lock().await;
                if active.as_deref() == Some(&session.id) {
                    active_identity = Some(state.identity.clone());
                }
                connections.push(json!({
                    "connection_id": session.id,
                    "active": active.as_deref() == Some(&session.id),
                    "in_transaction": state.in_transaction,
                    "running": session.running.load(Ordering::Acquire),
                    "identity": state.identity,
                }));
            }
            Ok::<_, anyhow::Error>(json!({
                "connections": connections,
                "_db": active_identity,
            }))
        }
        .await;
        result.map(tool_ok).unwrap_or_else(tool_error)
    }

    #[tool(description = "切换省略 connection_id 时使用的当前连接")]
    async fn switch_connection(
        &self,
        Parameters(args): Parameters<RequiredConnectionArgs>,
    ) -> CallToolResult {
        match self.sessions.switch(&args.connection_id).await {
            Ok(session) => {
                let state = session.state.lock().await;
                tool_ok(add_db(
                    json!({"switched": true}),
                    &state.identity,
                    ReconnectNotice::default(),
                ))
            }
            Err(error) => tool_error(error),
        }
    }

    #[tool(description = "断开连接；存在活动事务时拒绝，必须先 commit 或 rollback")]
    async fn disconnect(
        &self,
        Parameters(args): Parameters<RequiredConnectionArgs>,
    ) -> CallToolResult {
        let result = async {
            let session = self.sessions.get(Some(&args.connection_id)).await?;
            let mut state = session.state.lock().await;
            anyhow::ensure!(
                !state.in_transaction,
                "连接存在活动事务，请先 commit 或 rollback"
            );
            let identity = state.identity.clone();
            if let Some(conn) = state.conn.take() {
                conn.disconnect().await?;
            }
            drop(state);
            self.sessions.disconnect(&args.connection_id).await?;
            Ok::<_, anyhow::Error>(add_db(
                json!({"disconnected": true}),
                &identity,
                ReconnectNotice::default(),
            ))
        }
        .await;
        result.map(tool_ok).unwrap_or_else(tool_error)
    }

    #[tool(description = "通过独立控制连接执行 KILL QUERY，保留原会话和可保留的事务")]
    async fn cancel_statement(
        &self,
        Parameters(args): Parameters<ConnectionArgs>,
    ) -> CallToolResult {
        let result = async {
            let session = self.sessions.get(args.connection_id.as_deref()).await?;
            let thread_id = session.cancel().await?;
            let state = session.state.lock().await;
            Ok::<_, anyhow::Error>(add_db(
                json!({"cancelled": true, "thread_id": thread_id.to_string()}),
                &state.identity,
                ReconnectNotice::default(),
            ))
        }
        .await;
        result.map(tool_ok).unwrap_or_else(tool_error)
    }

    #[tool(description = "执行一条只读 SQL，按行数和字节数双重限制返回并排空剩余结果")]
    async fn query(&self, Parameters(args): Parameters<QueryArgs>) -> CallToolResult {
        self.run_query(args, true)
            .await
            .map(tool_ok)
            .unwrap_or_else(tool_error)
    }

    #[tool(description = "执行一条 DML/DDL/DCL；首条 DML 自动开启事务，DDL 隐式提交需显式确认")]
    async fn execute(&self, Parameters(args): Parameters<ExecuteArgs>) -> CallToolResult {
        let result = async {
            let kind = sql::classify(&args.sql)?;
            anyhow::ensure!(kind != StatementKind::Read, "只读 SQL 请使用 query");
            let params = bind_params(args.params)?;
            let session = self.sessions.get(args.connection_id.as_deref()).await?;
            let mut state = session.state.lock().await;
            let notice = session.reconnect_if_needed(&mut state).await?;
            if kind == StatementKind::ImplicitCommit {
                anyhow::ensure!(
                    !state.in_transaction,
                    "活动事务中拒绝执行可能隐式提交的 DDL/DCL；请先 commit 或 rollback"
                );
            } else if !state.in_transaction {
                state
                    .conn
                    .as_mut()
                    .expect("连接必须存在")
                    .query_drop("START TRANSACTION")
                    .await?;
                state.in_transaction = true;
            }
            let identity = state.identity.clone();
            let conn = state.conn.as_mut().expect("连接必须存在");
            let outcome = cancellable(&session, args.timeout_secs, async {
                let result = conn.exec_iter(args.sql, params).await?;
                let rows_affected = result.affected_rows();
                let last_insert_id = result.last_insert_id();
                let warnings = result.warnings();
                result.drop_result().await?;
                Ok((rows_affected, last_insert_id, warnings))
            })
            .await?;
            state.last_used = Instant::now();
            Ok::<_, anyhow::Error>(add_db(
                json!({
                    "executed": true,
                    "statement_kind": match kind {
                        StatementKind::Dml => "dml",
                        StatementKind::ImplicitCommit => "implicit_commit",
                        StatementKind::Read => unreachable!(),
                    },
                    "in_transaction": state.in_transaction,
                    "implicit_commit": kind == StatementKind::ImplicitCommit,
                    "rows_affected": outcome.0.to_string(),
                    "last_insert_id": outcome.1.map(|value| value.to_string()),
                    "warnings": outcome.2,
                }),
                &identity,
                notice,
            ))
        }
        .await;
        result.map(tool_ok).unwrap_or_else(tool_error)
    }

    #[tool(description = "预编译一次 DML 并执行多组参数；以保存点保证本批原子性")]
    async fn execute_many(&self, Parameters(args): Parameters<ExecuteManyArgs>) -> CallToolResult {
        let result = async {
            sql::require_dml(&args.sql)?;
            anyhow::ensure!(!args.params_list.is_empty(), "params_list 不能为空");
            let param_sets = args
                .params_list
                .into_iter()
                .map(|params| bind_params(Some(params)))
                .collect::<anyhow::Result<Vec<_>>>()?;
            let batch_size = param_sets.len();
            let session = self.sessions.get(args.connection_id.as_deref()).await?;
            let mut state = session.state.lock().await;
            let notice = session.reconnect_if_needed(&mut state).await?;
            if !state.in_transaction {
                state
                    .conn
                    .as_mut()
                    .unwrap()
                    .query_drop("START TRANSACTION")
                    .await?;
                state.in_transaction = true;
            }
            let identity = state.identity.clone();
            let savepoint = format!("mcp_batch_{}", SAVEPOINT_ID.fetch_add(1, Ordering::Relaxed));
            let conn = state.conn.as_mut().unwrap();
            let batch = cancellable(&session, args.timeout_secs, async {
                conn.query_drop(format!("SAVEPOINT {savepoint}")).await?;
                let statement = match conn.prep(&args.sql).await {
                    Ok(statement) => statement,
                    Err(error) => {
                        conn.query_drop(format!("ROLLBACK TO SAVEPOINT {savepoint}"))
                            .await?;
                        conn.query_drop(format!("RELEASE SAVEPOINT {savepoint}"))
                            .await?;
                        return Ok(Err((0_usize, error.to_string())));
                    }
                };
                let mut affected = 0_u64;
                for (index, params) in param_sets.into_iter().enumerate() {
                    match conn.exec_iter(&statement, params).await {
                        Ok(result) => {
                            affected = affected
                                .checked_add(result.affected_rows())
                                .ok_or_else(|| anyhow::anyhow!("批处理 rows_affected 超出 u64"))?;
                            if let Err(error) = result.drop_result().await {
                                conn.query_drop(format!("ROLLBACK TO SAVEPOINT {savepoint}"))
                                    .await?;
                                conn.query_drop(format!("RELEASE SAVEPOINT {savepoint}"))
                                    .await?;
                                return Ok(Err((index, error.to_string())));
                            }
                        }
                        Err(error) => {
                            conn.query_drop(format!("ROLLBACK TO SAVEPOINT {savepoint}"))
                                .await?;
                            conn.query_drop(format!("RELEASE SAVEPOINT {savepoint}"))
                                .await?;
                            return Ok(Err((index, error.to_string())));
                        }
                    }
                }
                conn.query_drop(format!("RELEASE SAVEPOINT {savepoint}"))
                    .await?;
                Ok(Ok(affected))
            })
            .await?;
            state.last_used = Instant::now();
            let body = match batch {
                Ok(affected) => add_db(
                    json!({
                        "executed": true,
                        "batch_size": batch_size,
                        "rows_affected": affected.to_string(),
                        "in_transaction": true,
                    }),
                    &identity,
                    notice,
                ),
                Err((index, error)) => add_db(
                    json!({
                        "ok": false,
                        "executed": false,
                        "failed_index": index,
                        "mysql_error": error,
                        "batch_rolled_back": true,
                        "in_transaction": true,
                    }),
                    &identity,
                    notice,
                ),
            };
            Ok::<_, anyhow::Error>(body)
        }
        .await;
        match result {
            Ok(value) if value["ok"] == json!(false) => CallToolResult::structured_error(value),
            Ok(value) => tool_ok(value),
            Err(error) => tool_error(error),
        }
    }

    #[tool(description = "提交当前连接由 execute 自动开启的事务")]
    async fn commit(&self, Parameters(args): Parameters<ConnectionArgs>) -> CallToolResult {
        self.finish_transaction(args.connection_id, true).await
    }

    #[tool(description = "回滚当前连接由 execute 自动开启的事务")]
    async fn rollback(&self, Parameters(args): Parameters<ConnectionArgs>) -> CallToolResult {
        self.finish_transaction(args.connection_id, false).await
    }

    #[tool(description = "将只读查询流式写入绝对路径 JSONL/CSV，正文不进入 MCP 返回体")]
    async fn query_to_file(&self, Parameters(args): Parameters<QueryToFileArgs>) -> CallToolResult {
        self.run_query_to_file(args)
            .await
            .map(tool_ok)
            .unwrap_or_else(tool_error)
    }

    #[tool(description = "列出可见数据库")]
    async fn list_databases(&self, Parameters(args): Parameters<ConnectionArgs>) -> CallToolResult {
        self.run_query(
            QueryArgs {
                sql: "SELECT SCHEMA_NAME AS database_name, DEFAULT_CHARACTER_SET_NAME AS default_character_set, DEFAULT_COLLATION_NAME AS default_collation FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME".to_owned(),
                params: None,
                max_rows: Some(10_000),
                max_bytes: Some(4 * 1024 * 1024),
                timeout_secs: None,
                connection_id: args.connection_id,
            },
            true,
        ).await.map(tool_ok).unwrap_or_else(tool_error)
    }

    #[tool(description = "列出数据库中的表和视图")]
    async fn list_tables(&self, Parameters(args): Parameters<ListTablesArgs>) -> CallToolResult {
        let params = vec![
            args.database
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
            args.database.map(Value::String).unwrap_or(Value::Null),
            args.name_like
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
            args.name_like.map(Value::String).unwrap_or(Value::Null),
        ];
        self.run_query(
            QueryArgs {
                sql: "SELECT TABLE_SCHEMA AS database_name, TABLE_NAME AS table_name, TABLE_TYPE AS table_type, ENGINE AS engine, TABLE_ROWS AS estimated_rows FROM information_schema.TABLES WHERE (? IS NULL OR TABLE_SCHEMA = ?) AND (? IS NULL OR TABLE_NAME LIKE ?) ORDER BY TABLE_SCHEMA, TABLE_NAME".to_owned(),
                params: Some(params),
                max_rows: args.max_rows,
                max_bytes: None,
                timeout_secs: None,
                connection_id: args.connection_id,
            },
            true,
        ).await.map(tool_ok).unwrap_or_else(tool_error)
    }

    #[tool(description = "描述表字段、类型、默认值、键和扩展属性")]
    async fn describe_table(
        &self,
        Parameters(args): Parameters<DescribeTableArgs>,
    ) -> CallToolResult {
        self.run_query(
            QueryArgs {
                sql: "SELECT ORDINAL_POSITION AS ordinal_position, COLUMN_NAME AS column_name, COLUMN_TYPE AS column_type, IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS column_default, COLUMN_KEY AS column_key, EXTRA AS extra, COLUMN_COMMENT AS column_comment FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION".to_owned(),
                params: Some(vec![Value::String(args.database), Value::String(args.table)]),
                max_rows: Some(10_000),
                max_bytes: Some(4 * 1024 * 1024),
                timeout_secs: None,
                connection_id: args.connection_id,
            },
            true,
        ).await.map(tool_ok).unwrap_or_else(tool_error)
    }

    #[tool(description = "列出 TABLE/VIEW/PROCEDURE/FUNCTION/TRIGGER/EVENT 对象")]
    async fn list_objects(&self, Parameters(args): Parameters<ListObjectsArgs>) -> CallToolResult {
        let result = self.build_list_objects(args).await;
        result.map(tool_ok).unwrap_or_else(tool_error)
    }

    #[tool(description = "取得 TABLE/VIEW/PROCEDURE/FUNCTION/TRIGGER/EVENT 的 SHOW CREATE DDL")]
    async fn get_object_ddl(&self, Parameters(args): Parameters<ObjectArgs>) -> CallToolResult {
        let result = async {
            let object = ObjectRef::from(&args);
            let (ddl, identity, notice) = self.fetch_ddl(&object, args.connection_id).await?;
            Ok::<_, anyhow::Error>(add_db(
                json!({"object": object, "ddl": ddl}),
                &identity,
                notice,
            ))
        }
        .await;
        result.map(tool_ok).unwrap_or_else(tool_error)
    }

    #[tool(description = "计算对象 DDL 的 SHA-256 指纹")]
    async fn object_fingerprint(&self, Parameters(args): Parameters<ObjectArgs>) -> CallToolResult {
        let result = async {
            let object = ObjectRef::from(&args);
            let (ddl, identity, notice) = self.fetch_ddl(&object, args.connection_id).await?;
            let sha256 = hex::encode(Sha256::digest(ddl.as_bytes()));
            Ok::<_, anyhow::Error>(add_db(
                json!({
                    "object": object,
                    "sha256": sha256,
                    "bytes": ddl.len().to_string(),
                }),
                &identity,
                notice,
            ))
        }
        .await;
        result.map(tool_ok).unwrap_or_else(tool_error)
    }

    #[tool(description = "按数据库/对象类型目录批量归档 DDL；单项失败不影响其他项")]
    async fn export_objects(
        &self,
        Parameters(args): Parameters<ExportObjectsArgs>,
    ) -> CallToolResult {
        self.run_export_objects(args)
            .await
            .map(tool_ok)
            .unwrap_or_else(tool_error)
    }
}

impl MysqlMcp {
    async fn finish_transaction(
        &self,
        connection_id: Option<String>,
        commit: bool,
    ) -> CallToolResult {
        let result = async {
            let session = self.sessions.get(connection_id.as_deref()).await?;
            let mut state = session.state.lock().await;
            let notice = session.reconnect_if_needed(&mut state).await?;
            anyhow::ensure!(state.in_transaction, "当前连接没有活动事务");
            let identity = state.identity.clone();
            let conn = state.conn.as_mut().unwrap();
            cancellable(&session, None, async {
                conn.query_drop(if commit { "COMMIT" } else { "ROLLBACK" })
                    .await?;
                Ok(())
            })
            .await?;
            state.in_transaction = false;
            state.last_used = Instant::now();
            let body = if commit {
                json!({"committed": true})
            } else {
                json!({"rolled_back": true})
            };
            Ok::<_, anyhow::Error>(add_db(body, &identity, notice))
        }
        .await;
        result.map(tool_ok).unwrap_or_else(tool_error)
    }

    async fn run_query_to_file(&self, args: QueryToFileArgs) -> anyhow::Result<Value> {
        sql::require_read(&args.sql)?;
        let format = args.format.to_ascii_lowercase();
        anyhow::ensure!(
            matches!(format.as_str(), "jsonl" | "csv"),
            "format 仅支持 jsonl 或 csv"
        );
        let params = bind_params(args.params)?;
        let (target, file) = AtomicTarget::create(&args.path, args.overwrite)?;
        let session = self.sessions.get(args.connection_id.as_deref()).await?;
        let mut state = session.state.lock().await;
        let notice = session.reconnect_if_needed(&mut state).await?;
        let identity = state.identity.clone();
        let conn = state.conn.as_mut().unwrap();
        let output = cancellable(&session, args.timeout_secs, async {
            let mut result = conn.exec_iter(args.sql, params).await?;
            let columns = result.columns().unwrap_or_default();
            let names = column_names(&columns)?;
            let mut writer = BufWriter::new(file);
            let mut hasher = Sha256::new();
            let mut bytes = 0_u64;
            let mut rows = 0_u64;
            if format == "csv" {
                let header = csv_record(names.iter().map(String::as_str))?;
                writer.write_all(&header)?;
                hasher.update(&header);
                bytes += header.len() as u64;
            }
            while let Some(row) = result.next().await? {
                let row = row_to_json(row, &columns, &names)?;
                let chunk = if format == "jsonl" {
                    let mut value = serde_json::to_vec(&row)?;
                    value.push(b'\n');
                    value
                } else {
                    let object = row.as_object().expect("row_to_json 必须返回对象");
                    csv_record(names.iter().map(|name| csv_cell(&object[name])))?
                };
                writer.write_all(&chunk)?;
                hasher.update(&chunk);
                bytes = bytes
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| anyhow::anyhow!("导出文件字节数超出 u64"))?;
                rows = rows
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("导出行数超出 u64"))?;
            }
            result.drop_result().await?;
            writer.flush()?;
            let file = writer.into_inner().map_err(|error| error.into_error())?;
            Ok((file, rows, bytes, hex::encode(hasher.finalize())))
        })
        .await?;
        let final_path = target.commit(output.0)?;
        state.last_used = Instant::now();
        Ok(add_db(
            json!({
                "path": final_path,
                "format": format,
                "rows": output.1.to_string(),
                "bytes": output.2.to_string(),
                "sha256": output.3,
            }),
            &identity,
            notice,
        ))
    }

    async fn build_list_objects(&self, args: ListObjectsArgs) -> anyhow::Result<Value> {
        let object_type = args
            .object_type
            .as_deref()
            .map(normalize_type)
            .transpose()?;
        let database = args.database.clone();
        let name_like = args.name_like.clone();
        let mut selects = Vec::new();
        let mut params = Vec::new();
        let requested: Vec<&str> = object_type
            .as_deref()
            .map(|kind| vec![kind])
            .unwrap_or_else(|| crate::objects::OBJECT_TYPES.to_vec());
        for kind in requested {
            let (schema_col, name_col, source, extra) = match kind {
                "TABLE" => (
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "information_schema.TABLES",
                    "TABLE_TYPE = 'BASE TABLE'",
                ),
                "VIEW" => (
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "information_schema.TABLES",
                    "TABLE_TYPE = 'VIEW'",
                ),
                "PROCEDURE" => (
                    "ROUTINE_SCHEMA",
                    "ROUTINE_NAME",
                    "information_schema.ROUTINES",
                    "ROUTINE_TYPE = 'PROCEDURE'",
                ),
                "FUNCTION" => (
                    "ROUTINE_SCHEMA",
                    "ROUTINE_NAME",
                    "information_schema.ROUTINES",
                    "ROUTINE_TYPE = 'FUNCTION'",
                ),
                "TRIGGER" => (
                    "TRIGGER_SCHEMA",
                    "TRIGGER_NAME",
                    "information_schema.TRIGGERS",
                    "1 = 1",
                ),
                "EVENT" => (
                    "EVENT_SCHEMA",
                    "EVENT_NAME",
                    "information_schema.EVENTS",
                    "1 = 1",
                ),
                _ => unreachable!(),
            };
            selects.push(format!("SELECT {schema_col} AS database_name, '{kind}' AS object_type, {name_col} AS object_name FROM {source} WHERE {extra} AND (? IS NULL OR {schema_col} = ?) AND (? IS NULL OR {name_col} LIKE ?)"));
            params.extend([
                database.clone().map(Value::String).unwrap_or(Value::Null),
                database.clone().map(Value::String).unwrap_or(Value::Null),
                name_like.clone().map(Value::String).unwrap_or(Value::Null),
                name_like.clone().map(Value::String).unwrap_or(Value::Null),
            ]);
        }
        let sql = format!(
            "{} ORDER BY database_name, object_type, object_name",
            selects.join(" UNION ALL ")
        );
        self.run_query(
            QueryArgs {
                sql,
                params: Some(params),
                max_rows: args.max_rows,
                max_bytes: None,
                timeout_secs: None,
                connection_id: args.connection_id,
            },
            true,
        )
        .await
    }

    async fn run_export_objects(&self, args: ExportObjectsArgs) -> anyhow::Result<Value> {
        let root = PathBuf::from(&args.out_dir);
        anyhow::ensure!(root.is_absolute(), "out_dir 必须是绝对路径");
        let paths = args
            .objects
            .iter()
            .map(|object| {
                let kind = normalize_type(&object.object_type)?;
                export_path(&root, &object.database, &kind, &object.name)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        validate_export_paths(&paths)?;
        std::fs::create_dir_all(&root)?;

        let mut statuses = Vec::new();
        let mut last_identity = None;
        for (object, path) in args.objects.iter().zip(paths) {
            let status = match self.fetch_ddl(object, args.connection_id.clone()).await {
                Ok((ddl, identity, _)) => {
                    last_identity = Some(identity);
                    match write_ddl(&path, &ddl, args.overwrite) {
                        Ok(sha256) => {
                            json!({"object": object, "ok": true, "path": path, "sha256": sha256})
                        }
                        Err(error) => {
                            json!({"object": object, "ok": false, "error": error.to_string()})
                        }
                    }
                }
                Err(error) => json!({"object": object, "ok": false, "error": error.to_string()}),
            };
            statuses.push(status);
        }
        let identity = match last_identity {
            Some(identity) => identity,
            None => {
                let session = self.sessions.get(args.connection_id.as_deref()).await?;
                session.state.lock().await.identity.clone()
            }
        };
        Ok(add_db(
            json!({"items": statuses}),
            &identity,
            ReconnectNotice::default(),
        ))
    }
}

fn csv_cell(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn csv_record<T: AsRef<[u8]>>(values: impl IntoIterator<Item = T>) -> anyhow::Result<Vec<u8>> {
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    writer.write_record(values)?;
    writer.flush()?;
    Ok(writer.into_inner()?)
}

fn write_ddl(path: &Path, ddl: &str, overwrite: bool) -> anyhow::Result<String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let (target, mut file) = AtomicTarget::create(
        path.to_str()
            .ok_or_else(|| anyhow::anyhow!("导出路径不是合法 UTF-8"))?,
        overwrite,
    )?;
    file.write_all(ddl.as_bytes())?;
    file.write_all(b"\n")?;
    let sha256 = hex::encode(Sha256::digest(ddl.as_bytes()));
    target.commit(file)?;
    Ok(sha256)
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for MysqlMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "MySQL MCP：使用 connect 建立运行时连接；query 只读，execute 写入后必须 commit 或 rollback；BIGINT/DECIMAL 已自动无损输出为字符串。",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::limit_reason;

    #[test]
    fn row_limit_takes_precedence_and_byte_limit_is_strict() {
        assert_eq!(limit_reason(200, 1, 1, 200, 32_768), Some("max_rows"));
        assert_eq!(limit_reason(1, 32_760, 9, 200, 32_768), Some("max_bytes"));
        assert_eq!(limit_reason(1, 32_760, 8, 200, 32_768), None);
    }
}
