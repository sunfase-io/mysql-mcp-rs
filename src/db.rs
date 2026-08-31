use mysql_async::{Conn, Opts, OptsBuilder, prelude::Queryable};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};

const DEFAULT_IDLE_SECS: u64 = 4 * 60 * 60;

#[derive(Clone)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl ConnectionConfig {
    fn opts(&self) -> Opts {
        OptsBuilder::default()
            .ip_or_hostname(self.host.clone())
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(Some(self.password.clone()))
            .db_name(Some(self.database.clone()))
            .into()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DbIdentity {
    pub connection_id: String,
    pub server_uuid: String,
    pub host_name: String,
    pub server_version: String,
    pub database: Option<String>,
    pub user: String,
    pub thread_id: String,
}

pub struct SessionState {
    pub conn: Option<Conn>,
    pub identity: DbIdentity,
    pub in_transaction: bool,
    pub transaction_lost: bool,
    pub last_used: Instant,
}

pub struct Session {
    pub id: String,
    pub config: ConnectionConfig,
    pub state: Mutex<SessionState>,
    pub running: AtomicBool,
    pub cancel_requested: AtomicBool,
    pub thread_id: AtomicU64,
    pub unusable: AtomicBool,
}

impl Session {
    async fn open(id: String, config: ConnectionConfig) -> anyhow::Result<Self> {
        let mut conn = Conn::new(config.opts()).await?;
        let identity = load_identity(&mut conn, &id, &config.user).await?;
        let thread_id = identity.thread_id.parse()?;
        Ok(Self {
            id,
            config,
            state: Mutex::new(SessionState {
                conn: Some(conn),
                identity,
                in_transaction: false,
                transaction_lost: false,
                last_used: Instant::now(),
            }),
            running: AtomicBool::new(false),
            cancel_requested: AtomicBool::new(false),
            thread_id: AtomicU64::new(thread_id),
            unusable: AtomicBool::new(false),
        })
    }

    pub async fn reconnect_if_needed(
        &self,
        state: &mut SessionState,
    ) -> anyhow::Result<ReconnectNotice> {
        let idle_expired = !state.in_transaction && state.last_used.elapsed() >= idle_timeout();
        let unusable = self.unusable.swap(false, Ordering::AcqRel);
        if !idle_expired && !unusable && state.conn.is_some() {
            state.last_used = Instant::now();
            return Ok(ReconnectNotice::default());
        }

        if state.in_transaction {
            state.transaction_lost = true;
            state.in_transaction = false;
        }
        state.conn.take();
        let mut conn = Conn::new(self.config.opts()).await?;
        let identity = load_identity(&mut conn, &self.id, &self.config.user).await?;
        self.thread_id
            .store(identity.thread_id.parse()?, Ordering::Release);
        state.identity = identity;
        state.conn = Some(conn);
        state.last_used = Instant::now();
        Ok(ReconnectNotice {
            reconnected: true,
            transaction_lost: std::mem::take(&mut state.transaction_lost),
        })
    }

    pub async fn cancel(&self) -> anyhow::Result<u64> {
        anyhow::ensure!(
            self.running.load(Ordering::Acquire),
            "连接 `{}` 当前没有正在执行的语句",
            self.id
        );
        let thread_id = self.thread_id.load(Ordering::Acquire);
        self.cancel_requested.store(true, Ordering::Release);
        match kill_query(&self.config, thread_id).await {
            Ok(()) => Ok(thread_id),
            Err(error) => {
                self.unusable.store(true, Ordering::Release);
                Err(anyhow::anyhow!(
                    "取消语句失败，连接已标记为不可复用: {error}"
                ))
            }
        }
    }

    fn reap_idle_connection(&self) {
        let Ok(mut state) = self.state.try_lock() else {
            return;
        };
        if !state.in_transaction && state.last_used.elapsed() >= idle_timeout() {
            state.conn.take();
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ReconnectNotice {
    pub reconnected: bool,
    pub transaction_lost: bool,
}

#[derive(Default)]
pub struct SessionManager {
    sessions: RwLock<HashMap<String, Arc<Session>>>,
    active: RwLock<Option<String>>,
}

impl SessionManager {
    pub fn start_idle_reaper(self: &Arc<Self>) {
        let manager = self.clone();
        tokio::spawn(async move {
            let interval = std::cmp::max(
                Duration::from_secs(1),
                std::cmp::min(idle_timeout(), Duration::from_secs(60)),
            );
            loop {
                tokio::time::sleep(interval).await;
                for session in manager.list().await {
                    session.reap_idle_connection();
                }
            }
        });
    }

    pub async fn connect(
        &self,
        id: String,
        config: ConnectionConfig,
    ) -> anyhow::Result<Arc<Session>> {
        anyhow::ensure!(!id.trim().is_empty(), "connection_id 不能为空");
        {
            let sessions = self.sessions.read().await;
            anyhow::ensure!(!sessions.contains_key(&id), "连接 `{id}` 已存在，拒绝覆盖");
        }
        let session = Arc::new(Session::open(id.clone(), config).await?);
        let mut sessions = self.sessions.write().await;
        anyhow::ensure!(!sessions.contains_key(&id), "连接 `{id}` 已存在，拒绝覆盖");
        sessions.insert(id.clone(), session.clone());
        drop(sessions);
        let mut active = self.active.write().await;
        if active.is_none() {
            *active = Some(id);
        }
        Ok(session)
    }

    pub async fn get(&self, requested: Option<&str>) -> anyhow::Result<Arc<Session>> {
        let id = match requested {
            Some(id) => id.to_owned(),
            None => self
                .active
                .read()
                .await
                .clone()
                .ok_or_else(|| anyhow::anyhow!("尚未建立数据库连接，请先调用 connect"))?,
        };
        self.sessions
            .read()
            .await
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("连接 `{id}` 不存在"))
    }

    pub async fn list(&self) -> Vec<Arc<Session>> {
        self.sessions.read().await.values().cloned().collect()
    }

    pub async fn active_id(&self) -> Option<String> {
        self.active.read().await.clone()
    }

    pub async fn switch(&self, id: &str) -> anyhow::Result<Arc<Session>> {
        let session = self.get(Some(id)).await?;
        *self.active.write().await = Some(id.to_owned());
        Ok(session)
    }

    pub async fn disconnect(&self, id: &str) -> anyhow::Result<Arc<Session>> {
        let session = self
            .sessions
            .write()
            .await
            .remove(id)
            .ok_or_else(|| anyhow::anyhow!("连接 `{id}` 不存在"))?;
        let mut active = self.active.write().await;
        if active.as_deref() == Some(id) {
            *active = self.sessions.read().await.keys().next().cloned();
        }
        Ok(session)
    }
}

pub async fn kill_query(config: &ConnectionConfig, thread_id: u64) -> anyhow::Result<()> {
    let mut control = Conn::new(config.opts()).await?;
    control
        .query_drop(format!("KILL QUERY {thread_id}"))
        .await?;
    control.disconnect().await?;
    Ok(())
}

async fn load_identity(
    conn: &mut Conn,
    connection_id: &str,
    user: &str,
) -> anyhow::Result<DbIdentity> {
    let (server_uuid, host_name, server_version, database, thread_id): (
        String,
        String,
        String,
        Option<String>,
        u64,
    ) = conn
        .query_first("SELECT @@server_uuid, @@hostname, VERSION(), DATABASE(), CONNECTION_ID()")
        .await?
        .ok_or_else(|| anyhow::anyhow!("MySQL 未返回连接身份"))?;
    Ok(DbIdentity {
        connection_id: connection_id.to_owned(),
        server_uuid,
        host_name,
        server_version,
        database,
        user: user.to_owned(),
        thread_id: thread_id.to_string(),
    })
}

fn idle_timeout() -> Duration {
    static IDLE: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *IDLE.get_or_init(|| {
        let seconds = std::env::var("MYSQL_MCP_IDLE_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_IDLE_SECS);
        Duration::from_secs(seconds)
    })
}
