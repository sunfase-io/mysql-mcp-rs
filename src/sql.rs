use sqlparser::{
    ast::{Query, SetExpr, Statement},
    dialect::MySqlDialect,
    parser::Parser,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    Read,
    Dml,
    ImplicitCommit,
}

pub fn classify(sql: &str) -> anyhow::Result<StatementKind> {
    let statements = Parser::parse_sql(&MySqlDialect {}, sql)
        .map_err(|error| anyhow::anyhow!("SQL 解析失败: {error}"))?;
    anyhow::ensure!(
        statements.len() == 1,
        "只允许单条 SQL，实际解析到 {} 条",
        statements.len()
    );

    let statement = &statements[0];
    reject_managed_statement(statement)?;
    Ok(match statement {
        Statement::Query(query) => {
            validate_read_query(query)?;
            StatementKind::Read
        }
        Statement::ShowFunctions { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowCreate { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowCatalogs { .. }
        | Statement::ShowDatabases { .. }
        | Statement::ShowProcessList { .. }
        | Statement::ShowSchemas { .. }
        | Statement::ShowCharset(_)
        | Statement::ShowObjects(_)
        | Statement::ShowTables { .. }
        | Statement::ShowViews { .. }
        | Statement::ShowCollation { .. }
        | Statement::ExplainTable { .. } => StatementKind::Read,
        Statement::Explain { statement, .. } => match statement.as_ref() {
            Statement::Query(query) => {
                validate_read_query(query)?;
                StatementKind::Read
            }
            _ => StatementKind::ImplicitCommit,
        },
        Statement::Insert(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::Merge(_)
        | Statement::LoadData { .. } => StatementKind::Dml,
        _ => StatementKind::ImplicitCommit,
    })
}

fn reject_managed_statement(statement: &Statement) -> anyhow::Result<()> {
    match statement {
        Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. } => {
            anyhow::bail!(
                "事务由 MCP 的 execute/commit/rollback 工具统一管理，禁止直接执行事务控制 SQL"
            )
        }
        Statement::Set(set) if set.to_string().to_ascii_lowercase().contains("autocommit") => {
            anyhow::bail!("禁止执行 SET autocommit；事务由 MCP 工具统一管理")
        }
        Statement::Set(sqlparser::ast::Set::SetTransaction { .. }) => {
            anyhow::bail!("事务由 MCP 工具统一管理，禁止直接执行 SET TRANSACTION")
        }
        Statement::Use(_) => {
            anyhow::bail!("禁止执行 USE；请在 connect 中指定默认库，跨库 SQL 使用 database.table")
        }
        Statement::Call(_) => {
            anyhow::bail!("拒绝执行 CALL：存储过程可在内部提交或回滚，无法可靠维护 MCP 事务状态")
        }
        _ => Ok(()),
    }
}

fn validate_read_query(query: &Query) -> anyhow::Result<()> {
    anyhow::ensure!(
        query.locks.is_empty(),
        "query 拒绝 FOR UPDATE/FOR SHARE 等锁定查询；它们需要显式事务语义"
    );
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            validate_read_query(&cte.query)?;
        }
    }
    validate_read_set_expr(&query.body)
}

fn validate_read_set_expr(expr: &SetExpr) -> anyhow::Result<()> {
    match expr {
        SetExpr::Select(select) => {
            anyhow::ensure!(
                select.into.is_none(),
                "query 拒绝 SELECT INTO；导出文件请使用 query_to_file"
            );
            Ok(())
        }
        SetExpr::Query(query) => validate_read_query(query),
        SetExpr::SetOperation { left, right, .. } => {
            validate_read_set_expr(left)?;
            validate_read_set_expr(right)
        }
        SetExpr::Values(_) | SetExpr::Table(_) => Ok(()),
        SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) | SetExpr::Merge(_) => {
            anyhow::bail!("query 查询体中包含 DML，拒绝作为只读 SQL 执行")
        }
    }
}

pub fn require_read(sql: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        classify(sql)? == StatementKind::Read,
        "query 只接受只读 SQL；DDL/DML/DCL 请使用 execute"
    );
    Ok(())
}

pub fn require_dml(sql: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        classify(sql)? == StatementKind::Dml,
        "execute_many 只接受一条 INSERT/UPDATE/DELETE/MERGE 参数化 DML"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_mysql_statements() {
        assert_eq!(classify("SELECT 1").unwrap(), StatementKind::Read);
        assert_eq!(
            classify("EXPLAIN SELECT * FROM demo").unwrap(),
            StatementKind::Read
        );
        assert_eq!(
            classify("UPDATE demo SET n = ? WHERE id = ?").unwrap(),
            StatementKind::Dml
        );
        assert_eq!(
            classify("ALTER TABLE demo ADD COLUMN x INT").unwrap(),
            StatementKind::ImplicitCommit
        );
    }

    #[test]
    fn rejects_multiple_and_transaction_sql() {
        assert!(classify("SELECT 1; SELECT 2").is_err());
        assert!(classify("BEGIN").is_err());
        assert!(classify("COMMIT").is_err());
        assert!(classify("SET autocommit = 1").is_err());
        assert!(classify("USE production").is_err());
        assert!(classify("CALL mutate_and_commit()").is_err());
        assert!(classify("SELECT * FROM demo FOR UPDATE").is_err());
        assert!(classify("SELECT * FROM demo FOR SHARE").is_err());
        assert!(classify("SELECT * FROM demo INTO OUTFILE '/tmp/demo.csv'").is_err());
    }
}
