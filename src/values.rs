use base64::{Engine, engine::general_purpose::STANDARD};
use mysql_async::{Column, Params, Row, Value as MysqlValue, consts::ColumnType};
use num_bigint::BigUint;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Map, Number, Value, json};
use std::collections::HashSet;

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ScalarParam {
    Null(()),
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    String(String),
}

pub fn bind_params(params: Option<Vec<ScalarParam>>) -> anyhow::Result<Params> {
    let values = params
        .unwrap_or_default()
        .into_iter()
        .map(bind_value)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Params::Positional(values))
}

fn bind_value(value: ScalarParam) -> anyhow::Result<MysqlValue> {
    Ok(match value {
        ScalarParam::Null(()) => MysqlValue::NULL,
        ScalarParam::Bool(value) => MysqlValue::Int(i64::from(value)),
        ScalarParam::String(value) => MysqlValue::Bytes(value.into_bytes()),
        ScalarParam::Unsigned(value) => {
            anyhow::ensure!(
                value <= MAX_SAFE_INTEGER,
                "数值参数 {value} 超过 JavaScript 安全整数范围，请改用字符串传入"
            );
            MysqlValue::UInt(value)
        }
        ScalarParam::Signed(value) => {
            anyhow::ensure!(
                value.unsigned_abs() <= MAX_SAFE_INTEGER,
                "数值参数 {value} 超过 JavaScript 安全整数范围，请改用字符串传入"
            );
            MysqlValue::Int(value)
        }
        ScalarParam::Float(value) => {
            anyhow::ensure!(value.is_finite(), "浮点参数必须是有限值");
            anyhow::ensure!(
                value.fract() != 0.0 || value.abs() <= MAX_SAFE_INTEGER as f64,
                "整数数值参数 {value} 超过 JavaScript 安全整数范围，请改用字符串传入"
            );
            MysqlValue::Double(value)
        }
    })
}

pub fn column_names(columns: &[Column]) -> anyhow::Result<Vec<String>> {
    let mut seen = HashSet::with_capacity(columns.len());
    columns
        .iter()
        .map(|column| {
            anyhow::ensure!(
                column.column_type() != ColumnType::MYSQL_TYPE_UNKNOWN,
                "结果包含未知 MySQL 列类型"
            );
            let name = std::str::from_utf8(column.name_ref())
                .map_err(|error| anyhow::anyhow!("结果列名不是合法 UTF-8: {error}"))?
                .to_owned();
            anyhow::ensure!(
                seen.insert(name.clone()),
                "结果包含重复列名 `{name}`，请在 SQL 中使用 AS 别名"
            );
            Ok(name)
        })
        .collect()
}

pub fn row_to_json(row: Row, columns: &[Column], names: &[String]) -> anyhow::Result<Value> {
    let values = row.unwrap();
    anyhow::ensure!(
        values.len() == columns.len(),
        "结果值数量与列元数据不一致: {} != {}",
        values.len(),
        columns.len()
    );
    let mut object = Map::with_capacity(values.len());
    for ((value, column), name) in values.into_iter().zip(columns).zip(names) {
        object.insert(name.clone(), value_to_json(value, column)?);
    }
    Ok(Value::Object(object))
}

pub fn value_to_json(value: MysqlValue, column: &Column) -> anyhow::Result<Value> {
    if matches!(value, MysqlValue::NULL) {
        return Ok(Value::Null);
    }

    use ColumnType::*;
    match column.column_type() {
        MYSQL_TYPE_LONGLONG => integer_string(value),
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => utf8_string(value),
        MYSQL_TYPE_BIT => bit_string(value),
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_INT24 | MYSQL_TYPE_LONG => {
            small_integer(value)
        }
        MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => finite_float(value),
        MYSQL_TYPE_DATE
        | MYSQL_TYPE_NEWDATE
        | MYSQL_TYPE_TIME
        | MYSQL_TYPE_TIMESTAMP
        | MYSQL_TYPE_DATETIME
        | MYSQL_TYPE_YEAR
        | MYSQL_TYPE_TIMESTAMP2
        | MYSQL_TYPE_DATETIME2
        | MYSQL_TYPE_TIME2 => temporal_string(value, column.column_type()),
        MYSQL_TYPE_JSON | MYSQL_TYPE_ENUM | MYSQL_TYPE_SET => utf8_string(value),
        MYSQL_TYPE_GEOMETRY | MYSQL_TYPE_VECTOR | MYSQL_TYPE_TYPED_ARRAY => binary_object(value),
        MYSQL_TYPE_TINY_BLOB
        | MYSQL_TYPE_MEDIUM_BLOB
        | MYSQL_TYPE_LONG_BLOB
        | MYSQL_TYPE_BLOB
        | MYSQL_TYPE_VARCHAR
        | MYSQL_TYPE_VAR_STRING
        | MYSQL_TYPE_STRING => {
            if is_binary(column) {
                binary_object(value)
            } else {
                utf8_string(value)
            }
        }
        MYSQL_TYPE_NULL => anyhow::bail!("列类型为 NULL，但服务端返回了非 NULL 值"),
        MYSQL_TYPE_UNKNOWN => anyhow::bail!("不支持未知 MySQL 列类型"),
    }
}

fn is_binary(column: &Column) -> bool {
    // MySQL 会给 information_schema.TABLE_NAME 等 UTF-8 系统列附带 BINARY_FLAG，
    // 该标志不能单独证明值是二进制。字符集 63（binary）才是 BINARY、VARBINARY
    // 和二进制 BLOB 的稳定判据。
    column.character_set() == 63
}

fn integer_string(value: MysqlValue) -> anyhow::Result<Value> {
    Ok(Value::String(match value {
        MysqlValue::Int(value) => value.to_string(),
        MysqlValue::UInt(value) => value.to_string(),
        MysqlValue::Bytes(value) => strict_utf8(value)?,
        other => anyhow::bail!("BIGINT 返回了不匹配的值类型: {other:?}"),
    }))
}

fn small_integer(value: MysqlValue) -> anyhow::Result<Value> {
    match value {
        MysqlValue::Int(value) => Ok(json!(value)),
        MysqlValue::UInt(value) => Ok(json!(value)),
        other => anyhow::bail!("整数列返回了不匹配的值类型: {other:?}"),
    }
}

fn finite_float(value: MysqlValue) -> anyhow::Result<Value> {
    let value = match value {
        MysqlValue::Float(value) => f64::from(value),
        MysqlValue::Double(value) => value,
        other => anyhow::bail!("浮点列返回了不匹配的值类型: {other:?}"),
    };
    anyhow::ensure!(value.is_finite(), "MySQL 返回了非有限浮点值");
    Ok(Value::Number(Number::from_f64(value).ok_or_else(|| {
        anyhow::anyhow!("无法编码浮点值 {value}")
    })?))
}

fn utf8_string(value: MysqlValue) -> anyhow::Result<Value> {
    match value {
        MysqlValue::Bytes(value) => Ok(Value::String(strict_utf8(value)?)),
        other => anyhow::bail!("文本列返回了不匹配的值类型: {other:?}"),
    }
}

fn strict_utf8(value: Vec<u8>) -> anyhow::Result<String> {
    String::from_utf8(value).map_err(|error| anyhow::anyhow!("MySQL 文本不是合法 UTF-8: {error}"))
}

fn bit_string(value: MysqlValue) -> anyhow::Result<Value> {
    let bytes = match value {
        MysqlValue::Bytes(value) => value,
        other => anyhow::bail!("BIT 列返回了不匹配的值类型: {other:?}"),
    };
    Ok(Value::String(
        BigUint::from_bytes_be(&bytes).to_str_radix(10),
    ))
}

fn binary_object(value: MysqlValue) -> anyhow::Result<Value> {
    match value {
        MysqlValue::Bytes(value) => Ok(json!({
            "encoding": "base64",
            "data": STANDARD.encode(value),
        })),
        other => anyhow::bail!("二进制列返回了不匹配的值类型: {other:?}"),
    }
}

fn temporal_string(value: MysqlValue, column_type: ColumnType) -> anyhow::Result<Value> {
    let text = match value {
        MysqlValue::Date(year, month, day, hour, minute, second, micros) => {
            let date = format!("{year:04}-{month:02}-{day:02}");
            if matches!(
                column_type,
                ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE
            ) {
                date
            } else if micros == 0 {
                format!("{date} {hour:02}:{minute:02}:{second:02}")
            } else {
                format!("{date} {hour:02}:{minute:02}:{second:02}.{micros:06}")
            }
        }
        MysqlValue::Time(negative, days, hour, minute, second, micros) => {
            let sign = if negative { "-" } else { "" };
            let hours = days * 24 + u32::from(hour);
            if micros == 0 {
                format!("{sign}{hours:02}:{minute:02}:{second:02}")
            } else {
                format!("{sign}{hours:02}:{minute:02}:{second:02}.{micros:06}")
            }
        }
        MysqlValue::Int(value) => value.to_string(),
        MysqlValue::UInt(value) => value.to_string(),
        MysqlValue::Bytes(value) => strict_utf8(value)?,
        other => anyhow::bail!("日期时间列返回了不匹配的值类型: {other:?}"),
    };
    Ok(Value::String(text))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(kind: ColumnType) -> Column {
        Column::new(kind).with_name(b"value")
    }

    #[test]
    fn preserves_bigint_decimal_and_bit_precision() {
        assert_eq!(
            value_to_json(
                MysqlValue::Int(i64::MAX),
                &column(ColumnType::MYSQL_TYPE_LONGLONG)
            )
            .unwrap(),
            json!("9223372036854775807")
        );
        assert_eq!(
            value_to_json(
                MysqlValue::UInt(u64::MAX),
                &column(ColumnType::MYSQL_TYPE_LONGLONG)
            )
            .unwrap(),
            json!("18446744073709551615")
        );
        assert_eq!(
            value_to_json(
                MysqlValue::Bytes(b"12345678901234567890.123456".to_vec()),
                &column(ColumnType::MYSQL_TYPE_NEWDECIMAL)
            )
            .unwrap(),
            json!("12345678901234567890.123456")
        );
        assert_eq!(
            value_to_json(
                MysqlValue::Bytes(vec![0xff, 0xff]),
                &column(ColumnType::MYSQL_TYPE_BIT)
            )
            .unwrap(),
            json!("65535")
        );
    }

    #[test]
    fn rejects_unsafe_or_composite_parameters() {
        assert!(bind_params(Some(vec![ScalarParam::Unsigned(9_007_199_254_740_992_u64)])).is_err());
        assert!(
            serde_json::from_value::<Vec<ScalarParam>>(json!([{"x": 1}])).is_err(),
            "对象参数必须在 MCP 参数反序列化阶段失败"
        );
    }

    #[test]
    fn scalar_parameter_schema_exposes_all_json_scalar_types() {
        let schema = serde_json::to_string(&schemars::schema_for!(ScalarParam)).unwrap();
        for expected in ["null", "boolean", "integer", "number", "string"] {
            assert!(
                schema.contains(expected),
                "Schema 缺少 {expected}: {schema}"
            );
        }
    }

    #[test]
    fn preserves_midnight_for_datetime_but_not_date() {
        let midnight = MysqlValue::Date(2026, 8, 31, 0, 0, 0, 0);
        assert_eq!(
            value_to_json(midnight.clone(), &column(ColumnType::MYSQL_TYPE_DATETIME)).unwrap(),
            json!("2026-08-31 00:00:00")
        );
        assert_eq!(
            value_to_json(midnight, &column(ColumnType::MYSQL_TYPE_DATE)).unwrap(),
            json!("2026-08-31")
        );
    }

    #[test]
    fn rejects_duplicate_column_names() {
        let columns = vec![
            Column::new(ColumnType::MYSQL_TYPE_LONG).with_name(b"id"),
            Column::new(ColumnType::MYSQL_TYPE_LONG).with_name(b"id"),
        ];
        assert!(column_names(&columns).is_err());
    }

    #[test]
    fn rejects_unknown_column_type_before_reading_rows() {
        let columns = vec![Column::new(ColumnType::MYSQL_TYPE_UNKNOWN).with_name(b"mystery")];
        assert!(column_names(&columns).is_err());
    }

    #[test]
    fn uses_binary_character_set_instead_of_binary_flag() {
        let text_column = Column::new(ColumnType::MYSQL_TYPE_VAR_STRING)
            .with_name(b"name")
            .with_flags(mysql_async::consts::ColumnFlags::BINARY_FLAG)
            .with_character_set(255);
        assert_eq!(
            value_to_json(MysqlValue::Bytes("中文".as_bytes().to_vec()), &text_column).unwrap(),
            json!("中文")
        );

        let binary_column = Column::new(ColumnType::MYSQL_TYPE_VAR_STRING)
            .with_name(b"payload")
            .with_character_set(63);
        assert_eq!(
            value_to_json(MysqlValue::Bytes(vec![0xff]), &binary_column).unwrap(),
            json!({"encoding": "base64", "data": "/w=="})
        );
    }
}
