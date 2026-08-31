mod db;
mod files;
mod objects;
mod server;
mod sql;
mod values;

use anyhow::Context;
use rmcp::{ServiceExt, transport::stdio};
use server::MysqlMcp;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    let service = MysqlMcp::new()
        .serve(stdio())
        .await
        .context("启动 MCP stdio 服务失败")?;
    service.waiting().await.context("MCP 服务异常退出")?;
    Ok(())
}
