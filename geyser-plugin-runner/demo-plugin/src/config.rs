use anyhow::Result;
use serde::Deserialize;
use std::fs::File;
use std::io::Read;

#[derive(Debug, Deserialize, Clone)]
pub struct GeyserPluginConfig {
    pub libpath: String,
    pub clickhouse: ClickHouseConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub username: String,
    pub password: String,
    pub batch_size: usize,
    pub panic_on_db_errors: bool,
    pub blocks_table: String,
    pub transactions_table: String,
    pub dump_failed_txs: bool,
}

impl GeyserPluginConfig {
    pub fn from_file(path: &str) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: GeyserPluginConfig = serde_json::from_str(&contents)?;
        Ok(config)
    }
}
