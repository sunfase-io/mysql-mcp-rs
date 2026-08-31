# mysql-mcp-rs

面向 Claude、Codex 等 MCP 客户端的 Rust MySQL 工具服务。它采用运行时连接、显式事务和无损 JSON 类型映射，重点解决旧 Node.js MySQL MCP 在 `BIGINT` / `DECIMAL` 上的精度丢失问题。

## 特性

- `mysql_async 0.37` 异步连接，不同连接可并行、同一连接严格串行。
- 每个数据库返回体同时提供 MCP `structured_content` 与纯 JSON 文本，并带 `_db` 身份回显。
- `BIGINT`、`BIGINT UNSIGNED`、`DECIMAL`、`NUMERIC`、`BIT` 始终输出无损十进制字符串。
- 第一条 DML 自动 `START TRANSACTION`，必须通过 `commit` / `rollback` 结束。
- 拒绝多语句 SQL、直接事务控制、`SET autocommit`，并阻止活动事务内的隐式提交 DDL/DCL。
- 查询按行数和字节数双重截断，仍会排空服务端结果，连接可以继续使用。
- `execute_many` 预编译一次并用保存点保证本批原子性。
- 超时和 `cancel_statement` 使用独立控制连接执行 `KILL QUERY`。
- `query_to_file` 流式原子写入 JSONL/CSV，返回行数、字节数与 SHA-256，不把正文塞进 Agent 上下文。
- 可列举、提取、指纹比较和批量归档 MySQL 对象 DDL。

## 构建

要求 Rust 1.88+（edition 2024，`rmcp 3.1` 的最低版本）。

```bash
git clone https://github.com/sunfase-io/mysql-mcp-rs
cd mysql-mcp-rs
cargo build --release
```

产物：

- Windows：`target/release/mysql-mcp.exe`
- macOS / Linux：`target/release/mysql-mcp`

### GitHub 自动发布

仓库的 `Release` 工作流由 GitHub 托管的 Windows、Linux、macOS runner 编译并上传压缩包与 SHA-256 校验文件，本机不需要预编译或上传二进制。

推送 `v*` 标签会自动发布，也可以使用 GitHub CLI 手动触发：

```bash
gh workflow run release.yml -f tag=v0.1.0
gh run watch --exit-status
```

手动触发时，工作流会在当前 `main` 提交上创建对应 tag 和 Release。Release 资产不可变；若 tag 已存在，必须改用新的版本号。

服务通过 stdio 使用 MCP 协议；数据库密码只保存在进程内存，不写配置、不回显、不记录日志。

服务不从环境变量读取数据库地址或凭据，也没有启动时固定数据源。每次需要新数据库时直接调用 `connect`：

```json
{
  "host": "<运行时地址>",
  "port": 3306,
  "user": "<运行时账号>",
  "password": "<运行时密码>",
  "database": "<运行时数据库>",
  "connection_id": "project-test"
}
```

可以继续用另一个 `connection_id` 连接不同主机或数据库，通过 `switch_connection` 切换默认连接；也可在任意 SQL 工具上显式传 `connection_id`，不同连接彼此独立并可并行。

## 客户端配置

CC-SWITCH 示例（路径请按本机实际位置修改）：

```json
{
  "command": "D:/RustroverProjects/mysql-mcp/target/release/mysql-mcp.exe"
}
```

本仓库不会修改 CC-SWITCH、Claude 或 Codex 配置。请由用户在对应客户端中粘贴并启用。

## 工具

| 分类 | 工具 | 说明 |
|---|---|---|
| 连接 | `connect` | 建立运行时连接；重复 `connection_id` 失败 |
| 连接 | `list_connections` | 列出身份、当前连接、事务和运行状态 |
| 连接 | `switch_connection` | 切换省略 `connection_id` 时使用的连接 |
| 连接 | `disconnect` | 断开无活动事务的连接 |
| 连接 | `cancel_statement` | 对当前语句执行 `KILL QUERY` |
| SQL | `query` | 只读查询，默认最多 200 行 / 32 KiB |
| SQL | `execute` | 单条 DML/DDL/DCL |
| SQL | `execute_many` | 一条 DML、多组参数、保存点原子批处理 |
| SQL | `commit` | 提交活动事务 |
| SQL | `rollback` | 回滚活动事务 |
| SQL | `query_to_file` | 流式导出 `jsonl` / `csv` |
| 元数据 | `list_databases` | 列数据库 |
| 元数据 | `list_tables` | 列表与视图 |
| 元数据 | `describe_table` | 列字段定义 |
| 对象 | `list_objects` | 列六类可归档对象 |
| 对象 | `get_object_ddl` | 取得 `SHOW CREATE` DDL |
| 对象 | `object_fingerprint` | 计算 DDL SHA-256 |
| 对象 | `export_objects` | 按库/类型目录批量归档 |

对象类型覆盖 `TABLE`、`VIEW`、`PROCEDURE`、`FUNCTION`、`TRIGGER`、`EVENT`。

## SQL 与事务规则

`query` 只接受解析为只读语句的单条 SQL；`execute` 不接受只读语句。参数使用 MySQL `?` 占位符，只接受 JSON 字符串、数字、布尔值和 `null`。超过 JavaScript 安全整数范围（`2^53 - 1`）的 numeric 参数会失败，请改传十进制字符串；数组和对象会失败。

第一条 `INSERT` / `UPDATE` / `DELETE` / `MERGE` 自动开启事务，随后所有 DML 保持未提交。无活动事务时调用 `commit` / `rollback` 会直接失败。活动事务中拒绝可能隐式提交的 DDL/DCL，避免之前的 DML 被意外提交。服务不会在断连后自动重放写操作、提交或任何用户 SQL。

空闲连接默认四小时后由后台任务回收，并在下次使用时按内存凭据重建；活动事务绝不因空闲而回收。可用 `MYSQL_MCP_IDLE_TIMEOUT_SECS` 调整空闲时间。

## 类型映射

映射严格依据 MySQL 列元数据：

- `BIGINT` / `BIGINT UNSIGNED` → JSON string，包括 `COUNT(*)`。
- `DECIMAL` / `NUMERIC` → 原始十进制 JSON string。
- `BIT` → 无损十进制 JSON string。
- `TINYINT` / `SMALLINT` / `MEDIUMINT` / `INT` → JSON number。
- `FLOAT` / `DOUBLE` → 有限 JSON number；非有限值失败。
- 日期时间、枚举、集合、MySQL `JSON` → JSON string；JSON 列不二次解析。
- 二进制、几何、向量 → `{ "encoding": "base64", "data": "..." }`。
- 重复列名、未知类型、非法 UTF-8、值与元数据类型不匹配 → 直接失败。

精度验收值：

```json
[
  "9223372036854775807",
  "18446744073709551615",
  "12345678901234567890.123456"
]
```

## 开发验证

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --release
```

连接真实 MySQL 的集成测试在启动后交互读取连接参数，密码输入不回显；不读取连接环境变量、不在仓库或日志保存凭据，也不得对正式库执行写操作。

```bash
node tests/mcp_driver.mjs
```

驱动只在当前 MCP 会话中创建 `TEMPORARY TABLE`，覆盖握手、中文/NULL/精度、参数绑定、事务、批处理保存点、超时与人工取消、截断排空、连接复用、元数据、JSONL/CSV 和对象 DDL 临时目录归档。

## License

[MIT](LICENSE)
