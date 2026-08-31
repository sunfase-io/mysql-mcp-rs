import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createInterface } from 'node:readline';
import { createInterface as createPrompt } from 'node:readline/promises';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';

async function readHiddenPassword(prompt) {
  if (!process.stdin.isTTY || typeof process.stdin.setRawMode !== 'function') {
    throw new Error('密码必须通过交互式终端输入，当前 stdin 不是 TTY');
  }
  process.stdout.write(prompt);
  process.stdin.setRawMode(true);
  process.stdin.resume();
  return await new Promise((resolvePromise, reject) => {
    let password = '';
    const finish = (error) => {
      process.stdin.off('data', onData);
      process.stdin.setRawMode(false);
      process.stdin.pause();
      process.stdout.write('\n');
      error ? reject(error) : resolvePromise(password);
    };
    const onData = (chunk) => {
      const value = chunk.toString('utf8');
      for (const character of value) {
        if (character === '\u0003') return finish(new Error('用户取消'));
        if (character === '\r' || character === '\n') return finish();
        if (character === '\u0008' || character === '\u007f') {
          if (password.length > 0) {
            password = password.slice(0, -1);
            process.stdout.write('\b \b');
          }
          continue;
        }
        password += character;
        process.stdout.write('*');
      }
    };
    process.stdin.on('data', onData);
  });
}

const prompt = createPrompt({ input: process.stdin, output: process.stdout });
const host = (await prompt.question('测试库 host: ')).trim();
const portText = (await prompt.question('测试库 port [3306]: ')).trim() || '3306';
const user = (await prompt.question('测试库 user: ')).trim();
const database = (await prompt.question('测试库 database: ')).trim();
prompt.close();
const password = await readHiddenPassword('测试库 password: ');
const port = Number(portText);
if (!host || !user || !database || !Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error('host/user/database 不能为空，port 必须是 1-65535 的整数');
}

const executable = process.argv[2]
  ?? resolve(process.cwd(), `target/release/mysql-mcp${process.platform === 'win32' ? '.exe' : ''}`);
const child = spawn(executable, [], { stdio: ['pipe', 'pipe', 'inherit'], windowsHide: true });
const lines = createInterface({ input: child.stdout });
const pending = new Map();
let requestId = 1;

lines.on('line', (line) => {
  const message = JSON.parse(line);
  if (message.id === undefined) return;
  const waiter = pending.get(message.id);
  if (!waiter) throw new Error(`收到未知响应 id=${message.id}`);
  pending.delete(message.id);
  message.error ? waiter.reject(new Error(JSON.stringify(message.error))) : waiter.resolve(message.result);
});

function send(message) {
  child.stdin.write(`${JSON.stringify(message)}\n`);
}

function request(method, params = {}) {
  const id = requestId++;
  return new Promise((resolvePromise, reject) => {
    pending.set(id, { resolve: resolvePromise, reject });
    send({ jsonrpc: '2.0', id, method, params });
  });
}

async function tool(name, args = {}, expectError = false) {
  const response = await request('tools/call', { name, arguments: args });
  if (!!response.isError !== expectError) {
    throw new Error(`${name} isError=${response.isError}: ${JSON.stringify(response)}`);
  }
  return response.structuredContent ?? JSON.parse(response.content[0].text);
}

function equal(actual, expected, label) {
  if (actual !== expected) throw new Error(`${label}: expected=${expected}, actual=${actual}`);
}

const connectionId = `integration-${process.pid}`;
const outputDir = mkdtempSync(resolve(tmpdir(), 'mysql-mcp-integration-'));

try {
  await request('initialize', {
    protocolVersion: '2025-06-18',
    capabilities: {},
    clientInfo: { name: 'mysql-mcp-integration', version: '1.0.0' },
  });
  send({ jsonrpc: '2.0', method: 'notifications/initialized', params: {} });

  const listed = await request('tools/list');
  const expectedTools = [
    'connect', 'list_connections', 'switch_connection', 'disconnect', 'cancel_statement',
    'query', 'execute', 'execute_many', 'commit', 'rollback', 'query_to_file',
    'list_databases', 'list_tables', 'describe_table', 'list_objects', 'get_object_ddl',
    'object_fingerprint', 'export_objects',
  ];
  for (const name of expectedTools) {
    if (!listed.tools.some((entry) => entry.name === name)) throw new Error(`缺少 MCP 工具 ${name}`);
  }
  const querySchema = JSON.stringify(listed.tools.find((entry) => entry.name === 'query')?.inputSchema);
  for (const scalarType of ['null', 'boolean', 'integer', 'number', 'string']) {
    if (!querySchema.includes(`"${scalarType}"`)) {
      throw new Error(`query params Schema 缺少 JSON 标量类型 ${scalarType}: ${querySchema}`);
    }
  }

  const connect = await tool('connect', {
    host,
    port,
    user,
    password,
    database,
    connection_id: connectionId,
  });
  equal(connect._db.connection_id, connectionId, 'connection identity');

  const secondConnectionId = `${connectionId}-second`;
  await tool('connect', { host, port, user, password, database, connection_id: secondConnectionId });
  await tool('switch_connection', { connection_id: secondConnectionId });
  const switchedResult = await tool('query', { sql: 'SELECT CONNECTION_ID() AS thread_id' });
  equal(switchedResult._db.connection_id, secondConnectionId, 'switch to second connection');
  await tool('switch_connection', { connection_id: connectionId });
  await tool('disconnect', { connection_id: secondConnectionId });

  await tool('execute', {
    connection_id: connectionId,
    sql: 'CREATE TEMPORARY TABLE mcp_integration (id BIGINT UNSIGNED PRIMARY KEY, txt VARCHAR(64), amount DECIMAL(26,6), nullable_col INT NULL)',
  });

  await tool('execute', {
    connection_id: connectionId,
    sql: 'INSERT INTO mcp_integration(id, txt, amount, nullable_col) VALUES (?, ?, ?, ?)',
    params: ['18446744073709551615', '中文', '12345678901234567890.123456', null],
  });
  const ddlRejected = await tool('execute', {
    connection_id: connectionId,
    sql: 'CREATE TEMPORARY TABLE mcp_must_not_exist (id INT)',
  }, true);
  equal(ddlRejected._db.connection_id, connectionId, 'DDL rejection database identity');
  await tool('rollback', { connection_id: connectionId });
  let result = await tool('query', {
    connection_id: connectionId,
    sql: 'SELECT COUNT(*) AS count FROM mcp_integration',
  });
  equal(result.rows[0].count, '0', 'rollback');

  await tool('execute_many', {
    connection_id: connectionId,
    sql: 'INSERT INTO mcp_integration(id, txt, amount, nullable_col) VALUES (?, ?, ?, ?)',
    params_list: [
      ['9223372036854775807', '第一行', '1.000001', null],
      ['18446744073709551615', '第二行', '12345678901234567890.123456', 7],
    ],
  });
  await tool('commit', { connection_id: connectionId });
  const duplicateCommit = await tool('commit', { connection_id: connectionId }, true);
  equal(duplicateCommit._db.connection_id, connectionId, 'commit error database identity');

  result = await tool('query', {
    connection_id: connectionId,
    sql: 'SELECT id, txt, amount, nullable_col FROM mcp_integration ORDER BY id',
  });
  equal(result.rows[0].id, '9223372036854775807', 'signed max precision');
  equal(result.rows[1].id, '18446744073709551615', 'unsigned max precision');
  equal(result.rows[1].amount, '12345678901234567890.123456', 'decimal precision');
  equal(result.rows[0].nullable_col, null, 'NULL mapping');

  const temporal = await tool('query', {
    connection_id: connectionId,
    sql: "SELECT CAST('2026-08-31 00:00:00' AS DATETIME) AS midnight_datetime, CAST('2026-08-31' AS DATE) AS date_only",
  });
  equal(temporal.rows[0].midnight_datetime, '2026-08-31 00:00:00', 'midnight DATETIME');
  equal(temporal.rows[0].date_only, '2026-08-31', 'DATE formatting');

  const lockingRead = await tool('query', {
    connection_id: connectionId,
    sql: 'SELECT id FROM mcp_integration FOR UPDATE',
  }, true);
  if (!/锁定查询/.test(lockingRead.error)) throw new Error(`未拒绝锁定查询: ${lockingRead.error}`);
  equal(lockingRead._db.connection_id, connectionId, 'locking query error database identity');

  const selectInto = await tool('query', {
    connection_id: connectionId,
    sql: "SELECT id FROM mcp_integration INTO OUTFILE '/tmp/mcp-must-not-write.csv'",
  }, true);
  if (!/SELECT INTO|SQL 解析失败/.test(selectInto.error)) {
    throw new Error(`未拒绝 SELECT INTO: ${selectInto.error}`);
  }

  const call = await tool('execute', {
    connection_id: connectionId,
    sql: 'CALL mcp_must_not_execute()',
  }, true);
  if (!/拒绝执行 CALL/.test(call.error)) throw new Error(`未拒绝 CALL: ${call.error}`);

  const rowTruncated = await tool('query', {
    connection_id: connectionId,
    sql: 'SELECT id FROM mcp_integration ORDER BY id',
    max_rows: 1,
  });
  equal(rowTruncated.truncated, true, 'row truncation');
  equal(rowTruncated.truncation_reason, 'max_rows', 'row truncation reason');
  const byteTruncated = await tool('query', {
    connection_id: connectionId,
    sql: "SELECT REPEAT('x', 100) AS wide_value",
    max_bytes: 8,
  });
  equal(byteTruncated.truncated, true, 'byte truncation');
  equal(byteTruncated.truncation_reason, 'max_bytes', 'byte truncation reason');

  const duplicate = await tool('query', {
    connection_id: connectionId,
    sql: 'SELECT 1 AS id, 2 AS id',
  }, true);
  if (!/重复列名/.test(duplicate.error)) throw new Error(`未拒绝重复列名: ${duplicate.error}`);
  const unsafeNumber = await tool('query', {
    connection_id: connectionId,
    sql: 'SELECT ? AS value',
    params: [9007199254740992],
  }, true);
  if (!/安全整数范围/.test(unsafeNumber.error)) throw new Error(`未拒绝非安全整数: ${unsafeNumber.error}`);

  const batchFailure = await tool('execute_many', {
    connection_id: connectionId,
    sql: 'INSERT INTO mcp_integration(id, txt, amount) VALUES (?, ?, ?)',
    params_list: [
      ['1', '本批应回滚', '1.0'],
      ['9223372036854775807', '主键冲突', '2.0'],
    ],
  }, true);
  equal(batchFailure.failed_index, 1, 'batch failure index');
  await tool('rollback', { connection_id: connectionId });
  result = await tool('query', {
    connection_id: connectionId,
    sql: 'SELECT COUNT(*) AS count FROM mcp_integration WHERE id = 1',
  });
  equal(result.rows[0].count, '0', 'savepoint rollback');

  const timedOut = await tool('query', {
    connection_id: connectionId,
    sql: 'SELECT SLEEP(3) AS slept',
    timeout_secs: 1,
  }, true);
  equal(timedOut._db.connection_id, connectionId, 'timeout error database identity');
  result = await tool('query', { connection_id: connectionId, sql: 'SELECT 1 AS alive' });
  equal(result.rows[0].alive, '1', 'connection reuse after timeout');

  const slowQuery = tool('query', {
    connection_id: connectionId,
    sql: 'SELECT SLEEP(10) AS slept',
  }, true);
  await new Promise((resolvePromise) => setTimeout(resolvePromise, 300));
  await tool('cancel_statement', { connection_id: connectionId });
  const cancelledQuery = await slowQuery;
  equal(cancelledQuery._db.connection_id, connectionId, 'cancel error database identity');
  result = await tool('query', { connection_id: connectionId, sql: 'SELECT 2 AS alive' });
  equal(result.rows[0].alive, '2', 'connection reuse after manual cancellation');

  const jsonlPath = resolve(outputDir, 'rows.jsonl');
  const exported = await tool('query_to_file', {
    connection_id: connectionId,
    sql: 'SELECT id, txt, amount FROM mcp_integration ORDER BY id',
    path: jsonlPath,
    format: 'jsonl',
  });
  equal(exported.rows, '2', 'query_to_file rows');
  if (!readFileSync(jsonlPath, 'utf8').includes('中文') && !readFileSync(jsonlPath, 'utf8').includes('第一行')) {
    throw new Error('JSONL 未保留中文');
  }
  const csvPath = resolve(outputDir, 'rows.csv');
  const csvExported = await tool('query_to_file', {
    connection_id: connectionId,
    sql: 'SELECT id, txt, amount FROM mcp_integration ORDER BY id',
    path: csvPath,
    format: 'csv',
  });
  equal(csvExported.rows, '2', 'CSV rows');
  if (!readFileSync(csvPath, 'utf8').startsWith('id,txt,amount')) throw new Error('CSV 缺少表头');

  await tool('list_databases', { connection_id: connectionId });
  await tool('list_tables', {
    connection_id: connectionId,
    database,
    max_rows: 5,
  });
  const objects = await tool('list_objects', {
    connection_id: connectionId,
    database,
    type: 'TABLE',
    max_rows: 5,
  });
  const firstTable = objects.rows[0];
  if (!firstTable) throw new Error('测试库没有可用于只读 DDL 归档验证的表');
  {
    await tool('describe_table', {
      connection_id: connectionId,
      database: firstTable.database_name,
      table: firstTable.object_name,
    });
    const object = {
      connection_id: connectionId,
      database: firstTable.database_name,
      type: firstTable.object_type,
      name: firstTable.object_name,
    };
    const ddl = await tool('get_object_ddl', object);
    await tool('object_fingerprint', object);
    const archive = await tool('export_objects', {
      connection_id: connectionId,
      out_dir: resolve(outputDir, 'objects'),
      objects: [{ database: object.database, type: object.type, name: object.name }],
    });
    equal(archive.items[0].ok, true, 'object archive');
    const archiveItem = archive.items[0];
    const fileHash = createHash('sha256').update(readFileSync(archiveItem.path)).digest('hex');
    const ddlHash = createHash('sha256').update(ddl.ddl).digest('hex');
    equal(archiveItem.file_sha256, fileHash, 'object archive file hash');
    equal(archiveItem.sha256, fileHash, 'object archive compatibility hash');
    equal(archiveItem.ddl_sha256, ddlHash, 'object archive DDL hash');
  }

  await tool('disconnect', { connection_id: connectionId });
  console.log('MCP 集成测试通过');
} finally {
  child.stdin.end();
  child.kill();
  rmSync(outputDir, { recursive: true, force: true });
}
