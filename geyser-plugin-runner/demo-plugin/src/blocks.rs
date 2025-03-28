use crate::config::ClickHouseConfig;
use anyhow::Result;
use clickhouse::inserter::Inserter;
use clickhouse::{Client, Row};
use serde::Serialize;
use serde_with::serde_as;
use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoVersions;
use std::fmt;
use std::time::Duration;
use tracing::info;

#[serde_as]
#[derive(Row, Serialize, Debug, Clone)]
pub struct BlockRow {
    pub ingest_time: i64,
    pub slot: u64,
    pub blockhash: String,
    pub rewards_count: usize,
    pub reward_pubkey: String,
    pub reward_lamports: i64,
    pub reward_post_balance: u64,
    pub reward_type: u8,
    pub reward_commission: u8,
    pub block_time: i64,
    pub block_height: u64,
    pub parent_slot: u64,
    pub parent_blockhash: String,
    pub executed_transaction_count: u64,
    pub entry_count: u64,
    pub block_version: String,
}

impl BlockRow {
    pub fn from_replica(block: ReplicaBlockInfoVersions) -> Result<Self> {
        // this is the time the block began processing and will be used to diff against ingest time in clickhouse
        let ingest_time = chrono::Utc::now();
        let ingest_time = ingest_time.timestamp_millis();
        match block {
            ReplicaBlockInfoVersions::V0_0_1(block) => {
                let rewards = block
                    .rewards
                    .iter()
                    .map(|r| {
                        (
                            r.pubkey.to_string(),
                            r.lamports,
                            r.post_balance,
                            r.reward_type.map(|rt| rt as u8).unwrap_or(0),
                            r.commission.unwrap_or(0),
                        )
                    })
                    .collect::<Vec<_>>();

                Ok(BlockRow {
                    ingest_time: ingest_time,
                    slot: block.slot,
                    blockhash: block.blockhash.to_string(),
                    rewards_count: rewards.len(),
                    reward_pubkey: rewards.get(0).map(|r| r.0.clone()).unwrap_or_default(), // default to empty string
                    reward_lamports: rewards.get(0).map(|r| r.1).unwrap_or(0), // default to 0
                    reward_post_balance: rewards.get(0).map(|r| r.2).unwrap_or(0), // default to 0
                    reward_type: rewards.get(0).map(|r| r.3).unwrap_or(0),     // default to 0
                    reward_commission: rewards.get(0).map(|r| r.4).unwrap_or(0), // default to 0
                    block_time: block.block_time.unwrap_or(0),
                    block_height: block.block_height.unwrap_or(0),
                    parent_slot: 0,                   // default to 0 if not present
                    parent_blockhash: "".to_string(), // default to empty string if not present
                    executed_transaction_count: 0,    // default to 0 if not present
                    entry_count: 0,                   // default to 0 if not present
                    block_version: "v0_0_1".to_string(),
                })
            }
            ReplicaBlockInfoVersions::V0_0_2(block) => {
                let rewards = block
                    .rewards
                    .iter()
                    .map(|r| {
                        (
                            r.pubkey.to_string(),
                            r.lamports,
                            r.post_balance,
                            r.reward_type.map(|rt| rt as u8).unwrap_or(0),
                            r.commission.unwrap_or(0),
                        )
                    })
                    .collect::<Vec<_>>();

                Ok(BlockRow {
                    ingest_time: ingest_time,
                    slot: block.slot,
                    blockhash: block.blockhash.to_string(),
                    rewards_count: rewards.len(), // count of rewards
                    reward_pubkey: rewards.get(0).map(|r| r.0.clone()).unwrap_or_default(), // default to empty string
                    reward_lamports: rewards.get(0).map(|r| r.1).unwrap_or(0), // default to 0
                    reward_post_balance: rewards.get(0).map(|r| r.2).unwrap_or(0), // default to 0
                    reward_type: rewards.get(0).map(|r| r.3).unwrap_or(0),     // default to 0
                    reward_commission: rewards.get(0).map(|r| r.4).unwrap_or(0), // default to 0
                    block_time: block.block_time.unwrap_or(0),
                    block_height: block.block_height.unwrap_or(0),
                    parent_slot: block.parent_slot,
                    parent_blockhash: block.parent_blockhash.to_string(),
                    executed_transaction_count: block.executed_transaction_count,
                    entry_count: 0, // default to 0 if not present
                    block_version: "v0_0_2".to_string(), // block version
                })
            }
            ReplicaBlockInfoVersions::V0_0_3(block) => {
                let rewards = block
                    .rewards
                    .iter()
                    .map(|r| {
                        (
                            r.pubkey.to_string(),
                            r.lamports,
                            r.post_balance,
                            r.reward_type.map(|rt| rt as u8).unwrap_or(0),
                            r.commission.unwrap_or(0),
                        )
                    })
                    .collect::<Vec<_>>();

                Ok(BlockRow {
                    ingest_time: ingest_time,
                    slot: block.slot,
                    blockhash: block.blockhash.to_string(),
                    rewards_count: rewards.len(), // number of rewards
                    reward_pubkey: rewards.get(0).map(|r| r.0.clone()).unwrap_or_default(), // default to empty string
                    reward_lamports: rewards.get(0).map(|r| r.1).unwrap_or(0), // default to 0
                    reward_post_balance: rewards.get(0).map(|r| r.2).unwrap_or(0), // default to 0
                    reward_type: rewards.get(0).map(|r| r.3).unwrap_or(0),     // default to 0
                    reward_commission: rewards.get(0).map(|r| r.4).unwrap_or(0), // default to 0
                    block_time: block.block_time.unwrap_or(0) * 1000, // clickhouse expects unix timestamp in milliseconds
                    block_height: block.block_height.unwrap_or(0),
                    parent_slot: block.parent_slot,
                    parent_blockhash: block.parent_blockhash.to_string(),
                    executed_transaction_count: block.executed_transaction_count,
                    entry_count: block.entry_count, // default to 0 if not present
                    block_version: "v0_0_3".to_string(), // block version
                })
            }
            ReplicaBlockInfoVersions::V0_0_4(block) => {
                let rewards = block
                    .rewards
                    .rewards
                    .iter()
                    .map(|r| {
                        (
                            r.pubkey.to_string(),
                            r.lamports,
                            r.post_balance,
                            r.reward_type.map(|rt| rt as u8).unwrap_or(0),
                            r.commission.unwrap_or(0),
                        )
                    })
                    .collect::<Vec<_>>();

                Ok(BlockRow {
                    ingest_time: ingest_time,
                    slot: block.slot,
                    blockhash: block.blockhash.to_string(),
                    rewards_count: rewards.len(), // number of rewards
                    reward_pubkey: rewards.get(0).map(|r| r.0.clone()).unwrap_or_default(), // default to empty string
                    reward_lamports: rewards.get(0).map(|r| r.1).unwrap_or(0), // default to 0
                    reward_post_balance: rewards.get(0).map(|r| r.2).unwrap_or(0), // default to 0
                    reward_type: rewards.get(0).map(|r| r.3).unwrap_or(0),     // default to 0
                    reward_commission: rewards.get(0).map(|r| r.4).unwrap_or(0), // default to 0
                    block_time: block.block_time.unwrap_or(0) * 1000, // clickhouse expects unix timestamp in milliseconds
                    block_height: block.block_height.unwrap_or(0),
                    parent_slot: block.parent_slot,
                    parent_blockhash: block.parent_blockhash.to_string(),
                    executed_transaction_count: block.executed_transaction_count,
                    entry_count: block.entry_count, // default to 0 if not present
                    block_version: "v0_0_4".to_string(), // block version
                })
            }
        }
    }
}

pub struct BlockWriter {
    inserter: Inserter<BlockRow>,
    total_blocks: usize,
}

impl BlockWriter {
    pub fn new(config: ClickHouseConfig) -> Result<Self> {
        let client = Client::default()
            .with_url(&config.url)
            .with_database(&config.database)
            .with_user(&config.username)
            .with_password(&config.password);

        let inserter = client
            .inserter(&config.blocks_table)?
            .with_timeouts(Some(Duration::from_secs(10)), Some(Duration::from_secs(30)))
            .with_max_bytes(50_000_000)
            .with_max_rows(10000)
            .with_period(Some(Duration::from_secs(1)));

        Ok(Self {
            inserter,
            total_blocks: 0,
        })
    }

    pub async fn add_block(&mut self, block_row: BlockRow) -> Result<()> {
        self.inserter.write(&block_row)?;
        self.total_blocks += 1;

        let quantities = self.inserter.commit().await?;
        if quantities.rows > 0 {
            info!(
                "Flushed {} blocks, total blocks: {}",
                quantities.rows, self.total_blocks
            );
        }

        Ok(())
    }
}

impl fmt::Debug for BlockWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockWriter")
            .field("total_blocks", &self.total_blocks)
            .finish()
    }
}
