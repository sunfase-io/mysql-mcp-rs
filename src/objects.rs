use serde::{Deserialize, Serialize};

pub const OBJECT_TYPES: &[&str] = &["TABLE", "VIEW", "PROCEDURE", "FUNCTION", "TRIGGER", "EVENT"];

#[derive(Debug, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ObjectRef {
    pub database: String,
    #[serde(rename = "type")]
    pub object_type: String,
    pub name: String,
}

pub fn normalize_type(value: &str) -> anyhow::Result<String> {
    let value = value.trim().to_ascii_uppercase();
    anyhow::ensure!(
        OBJECT_TYPES.contains(&value.as_str()),
        "不支持的对象类型: {value}"
    );
    Ok(value)
}

pub fn quote_ident(value: &str) -> anyhow::Result<String> {
    anyhow::ensure!(!value.is_empty(), "标识符不能为空");
    anyhow::ensure!(!value.contains('\0'), "标识符不能包含 NUL");
    Ok(format!("`{}`", value.replace('`', "``")))
}

pub fn show_create_sql(object: &ObjectRef) -> anyhow::Result<String> {
    let kind = normalize_type(&object.object_type)?;
    let database = quote_ident(&object.database)?;
    let name = quote_ident(&object.name)?;
    Ok(match kind.as_str() {
        "TABLE" | "VIEW" => format!("SHOW CREATE TABLE {database}.{name}"),
        "PROCEDURE" => format!("SHOW CREATE PROCEDURE {database}.{name}"),
        "FUNCTION" => format!("SHOW CREATE FUNCTION {database}.{name}"),
        "TRIGGER" => format!("SHOW CREATE TRIGGER {database}.{name}"),
        "EVENT" => format!("SHOW CREATE EVENT {database}.{name}"),
        _ => unreachable!(),
    })
}

pub fn find_ddl(row: &serde_json::Value) -> anyhow::Result<String> {
    let object = row
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("SHOW CREATE 未返回 JSON 对象"))?;
    object
        .iter()
        .find_map(|(name, value)| {
            let key = name.to_ascii_lowercase();
            (key.starts_with("create ") || key == "sql original statement")
                .then(|| value.as_str().map(str::to_owned))
                .flatten()
        })
        .ok_or_else(|| anyhow::anyhow!("SHOW CREATE 结果中未找到 DDL 字段"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn quotes_mysql_identifiers() {
        assert_eq!(quote_ident("a`b").unwrap(), "`a``b`");
    }

    #[test]
    fn extracts_show_create_ddl() {
        assert_eq!(
            find_ddl(&json!({"Table": "t", "Create Table": "CREATE TABLE `t` (`id` int)"}))
                .unwrap(),
            "CREATE TABLE `t` (`id` int)"
        );
    }
}
