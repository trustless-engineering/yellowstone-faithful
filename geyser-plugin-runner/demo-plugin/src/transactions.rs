use crate::config::ClickHouseConfig;
use anyhow::Result;
use clickhouse::{Client, Row};
use log::{error, info};
use serde::Serialize;
use serde_with::serde_as;
use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoVersions;
use solana_sdk::bs58;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::reward_type::RewardType;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::sync::Arc;
use tokio::sync::Mutex;

/// represents a transaction message with its associated slot
#[serde_as]
#[derive(Row, Serialize, Debug, Clone, Default)]
pub struct TransactionMessage {
    pub transaction: TransactionRow,
    pub slot: u64,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct Reward {
    pub pubkey: String,
    pub lamports: i64,
    pub post_balance: i64,
    pub reward_type: u8,
    pub commission: u8,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct ReturnData {
    pub program_id: String,
    pub data: String,
}

/// represents a single token balance entry
#[derive(Serialize, Debug, Clone, Default)]
pub struct TokenBalance {
    pub account_index: i64,
    pub mint: String,
    pub owner: String,
    pub amount: String, // stored as string to handle high precision
    pub decimals: i64,
}

/// represents the detailed structure of a transaction
#[derive(Row, Serialize, Debug, Clone, Default)]
pub struct TransactionRow {
    // this rename is because clickhouse relies on field order unless explicitly
    // mapped to column names during insertion
    #[serde(rename = "ingest_time")]
    pub ingest_time: i64,

    #[serde(rename = "signature")]
    pub signature: String,

    #[serde(rename = "signatures")]
    pub signatures: Vec<String>,

    #[serde(rename = "signers")]
    pub signers: Vec<String>,

    #[serde(rename = "unique_signers")]
    pub unique_signers: Vec<String>,

    #[serde(rename = "is_vote")]
    pub is_vote: bool,

    #[serde(rename = "index")]
    pub index: u64,

    #[serde(rename = "num_required_signatures")]
    pub num_required_signatures: u8,

    #[serde(rename = "num_readonly_signed_accounts")]
    pub num_readonly_signed_accounts: u8,

    #[serde(rename = "num_readonly_unsigned_accounts")]
    pub num_readonly_unsigned_accounts: u8,

    #[serde(rename = "account_keys")]
    pub account_keys: Vec<String>,

    #[serde(rename = "recent_blockhash")]
    pub recent_blockhash: String,

    #[serde(rename = "instructions")]
    pub instructions: Vec<(u8, Vec<String>, String)>,

    #[serde(rename = "success")]
    pub success: bool,

    #[serde(rename = "error")]
    pub error: String,

    #[serde(rename = "fee")]
    pub fee: i64,

    #[serde(rename = "pre_balances")]
    pub pre_balances: Vec<i64>,

    #[serde(rename = "post_balances")]
    pub post_balances: Vec<i64>,

    #[serde(rename = "inner_instructions")]
    pub inner_instructions: Vec<(Vec<String>, String)>,

    #[serde(rename = "log_messages")]
    pub log_messages: Vec<String>,

    #[serde(rename = "pre_token_balances")]
    pub pre_token_balances: Vec<TokenBalance>,

    #[serde(rename = "post_token_balances")]
    pub post_token_balances: Vec<TokenBalance>,

    #[serde(rename = "rewards")]
    pub rewards: Vec<Reward>,

    #[serde(rename = "loaded_addresses")]
    pub loaded_addresses: Vec<String>,

    #[serde(rename = "return_data")]
    pub return_data: Vec<ReturnData>,

    #[serde(rename = "compute_units_consumed")]
    pub compute_units_consumed: i64,

    #[serde(rename = "transaction_version")]
    pub transaction_version: String,
}

impl TransactionRow {
    /// creates a transactionrow from replica transaction info
    pub fn from_replica(replica: ReplicaTransactionInfoVersions<'_>) -> Result<Self> {
        // this is the time the transaction began processing, used to diff against ingest time at clickhouse
        let ingest_time = chrono::Utc::now().timestamp_millis();

        // extract necessary fields based on the replica version
        let (signature, is_vote, tx, meta, index) = match replica {
            ReplicaTransactionInfoVersions::V0_0_1(info) => (
                info.signature.to_string(),
                info.is_vote,
                info.transaction.clone(),
                info.transaction_status_meta.clone(),
                0,
            ),
            ReplicaTransactionInfoVersions::V0_0_2(info) => (
                info.signature.to_string(),
                info.is_vote,
                info.transaction.clone(),
                info.transaction_status_meta.clone(),
                info.index,
            ),
        };

        // TODO:
        // this is a lazy and retarded way of determining transaction version
        // but it works for now.
        let mut transaction_version = "v0_0_1".to_string();
        if index > 0 {
            transaction_version = "v0_0_2".to_string();
        } else {
            transaction_version = "v0_0_1".to_string();
        }

        // determine transaction success
        let success = meta.status.is_ok();

        let message = tx.message();
        let all_signers: Vec<Pubkey> = message
            .instructions()
            .iter()
            .enumerate()
            .flat_map(|(ix_index, _)| message.get_ix_signers(ix_index))
            .cloned()
            .collect();

        let signatures = tx.signatures().iter().map(|s| s.to_string()).collect();

        // signers ends up as an array of pubkeys in string form. because we're iterating over the
        // instructions, this ends up being an array of the same pubkey repeated for each instruction
        // most of the time.
        // --
        // this is fine and means the position in the array aligns with the instruction associated
        // with it. this is useful if you've scenarios where multiple pubkeys are signing the same
        // instruction, but it means we need to flatten the array to get the correct signer for each
        // instruction.
        let signers = all_signers
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        // to get unique signers, we can flatten the array and use a hashset to remove duplicates. this will
        // be useful for more easily searching an array for a specific signer. we can collect directly into
        // a vec while ensuring uniqueness by passing through a hashset
        let unique_signers: Vec<String> = signers
            .iter()
            .cloned()
            .collect::<HashSet<String>>()
            .into_iter()
            .collect();

        let account_keys = message
            .account_keys()
            .iter()
            .map(|k| k.to_string())
            .collect();
        let recent_blockhash = message.recent_blockhash().to_string();

        let tx_error = meta.status.err().map(|e| e.to_string()).unwrap_or_default();

        let pre_balances = meta.pre_balances.iter().map(|&b| b as i64).collect();
        let post_balances = meta.post_balances.iter().map(|&b| b as i64).collect();

        // populate pre_token_balances using tokenbalance struct
        let pre_token_balances = meta
            .pre_token_balances
            .unwrap_or_default()
            .iter()
            .map(|tb| TokenBalance {
                account_index: tb.account_index as i64,
                mint: tb.mint.clone(),
                owner: tb.owner.clone(),
                amount: tb.ui_token_amount.amount.clone(),
                decimals: tb.ui_token_amount.decimals.into(),
            })
            .collect();

        // populate post_token_balances using tokenbalance struct
        let post_token_balances = meta
            .post_token_balances
            .unwrap_or_default()
            .iter()
            .map(|tb| TokenBalance {
                account_index: tb.account_index as i64,
                mint: tb.mint.clone(),
                owner: tb.owner.clone(),
                amount: tb.ui_token_amount.amount.clone(),
                decimals: tb.ui_token_amount.decimals.into(),
            })
            .collect();

        let loaded_addresses = meta
            .loaded_addresses
            .writable
            .iter()
            .chain(meta.loaded_addresses.readonly.iter())
            .map(|addr| addr.to_string())
            .collect();

        // intentionally leaving return_data empty
        // uncomment and implement return_data as needed

        let return_data = meta
            .return_data
            .unwrap_or_default()
            .data
            .iter()
            .map(|r| ReturnData {
                program_id: r.to_string(),
                data: bs58::encode(&r.to_string()).into_string(),
            })
            .collect::<Vec<ReturnData>>();

        #[allow(unreachable_patterns)]
        let rewards = meta
            .rewards
            .unwrap_or_default()
            .iter()
            .map(|r| Reward {
                pubkey: r.pubkey.to_string(),
                lamports: r.lamports,
                post_balance: r.post_balance as i64,
                reward_type: r
                    .reward_type
                    .map(|rt| match rt {
                        RewardType::Fee => 1,
                        RewardType::Rent => 2,
                        RewardType::Staking => 3,
                        RewardType::Voting => 4,
                        _ => 0,
                    })
                    .unwrap_or(0),
                commission: r.commission.unwrap_or(0),
            })
            .collect();

        let compute_units_consumed = meta.compute_units_consumed.unwrap_or_default() as i64;

        let transaction_row = TransactionRow {
            ingest_time,
            signature,
            signatures,
            signers,
            is_vote,
            index: index as u64,
            num_required_signatures: message.header().num_required_signatures,
            num_readonly_signed_accounts: message.header().num_readonly_signed_accounts,
            num_readonly_unsigned_accounts: message.header().num_readonly_unsigned_accounts,
            account_keys,
            recent_blockhash,
            instructions: vec![],
            success,
            error: tx_error,
            fee: meta.fee as i64,
            pre_balances,
            post_balances,
            pre_token_balances,
            post_token_balances,
            inner_instructions: vec![],
            log_messages: meta.log_messages.unwrap_or_default(),
            rewards,
            loaded_addresses,
            return_data,
            compute_units_consumed,
            transaction_version,
            unique_signers,
        };

        Ok(transaction_row)
    }
}
/// manages writing transactions to clickhouse with batching by slot
#[derive(Clone)]
pub struct TransactionWriter {
    client: Arc<Client>,
    config: ClickHouseConfig,
    failed_log: Arc<Mutex<std::fs::File>>,
    // shared state for batching
    state: Arc<Mutex<BatchState>>,
}

/// represents the current state of batching
#[derive(Debug, Default)]
struct BatchState {
    current_slot: Option<u64>,
    buffer: Vec<TransactionRow>,
}

impl TransactionWriter {
    /// creates a new transaction writer with the given configuration
    pub fn new(config: ClickHouseConfig) -> Result<Self> {
        // initialize the clickhouse client
        let client = Client::default()
            .with_url(&config.url)
            .with_database(&config.database)
            .with_user(&config.username)
            .with_password(&config.password)
            .with_option("format_binary_max_string_size", "10000000000"); // set to 10gb so the base64 encoded strings do not cause issues

        // open the failed transactions log file
        let failed_log = Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open("/mnt/misc/failed-txs.jsonl")?,
        ));

        Ok(Self {
            client: Arc::new(client),
            config,
            failed_log,
            state: Arc::new(Mutex::new(BatchState::default())),
        })
    }

    /// flushes the current batch to clickhouse
    async fn flush(&self, batch: BatchState) -> Result<()> {
        if batch.buffer.is_empty() {
            return Ok(());
        }

        // create a new inserter for the batch
        let table = &self.config.transactions_table;
        let mut inserter = self.client.insert(table)?;

        // iterate over each transaction and write
        for tx in &batch.buffer {
            if let Err(e) = inserter.write(tx).await {
                error!("failed to write transaction {}: {:?}", tx.signature, e);
                // log the failed transaction
                let log_entry = serde_json::to_string(tx)?;
                let mut failed_log = self.failed_log.lock().await;
                use std::io::Write;
                writeln!(failed_log, "{}", log_entry)?;
            }
        }

        // attempt to commit the inserter
        if let Err(e) = inserter.end().await {
            error!(
                "failed to commit batch for slot {}: {:?}",
                batch.current_slot.unwrap_or(0),
                e
            );

            let mut failed_log = self.failed_log.lock().await;
            use std::io::Write;
            writeln!(
                failed_log,
                "failed to commit batch for slot {}: {:?}",
                batch.current_slot.unwrap_or(0),
                e
            )?;

            if self.config.dump_failed_txs {
                // log all transactions in the batch as failed
                writeln!(failed_log, "failed transactions in batch:")?;
                for tx in &batch.buffer {
                    // consider logging more details if necessary
                    let log_entry = serde_json::to_string(&tx.signature)?;
                    writeln!(failed_log, "{}", log_entry)?;
                }
            }
        } else {
            info!(
                "flushed {} transactions for slot {}",
                batch.buffer.len(),
                batch.current_slot.unwrap_or(0)
            );
        }

        Ok(())
    }

    /// adds a transaction to the current batch, flushing if the slot changes
    pub async fn add_transaction(&self, transaction_message: TransactionMessage) -> Result<()> {
        let mut state = self.state.lock().await;

        match state.current_slot {
            None => {
                // first transaction being processed
                state.current_slot = Some(transaction_message.slot);
                state.buffer.push(transaction_message.transaction.clone());
                info!("started new batch for slot {}", transaction_message.slot);
            }
            Some(current_slot) => {
                if transaction_message.slot == current_slot {
                    // same slot, add to buffer
                    state.buffer.push(transaction_message.transaction.clone());
                } else {
                    // different slot, flush current buffer
                    let batch = BatchState {
                        current_slot: state.current_slot,
                        buffer: std::mem::take(&mut state.buffer),
                    };

                    // release the lock before flushing to allow other operations
                    drop(state);
                    self.flush(batch).await?;

                    // re-acquire the lock to update the state
                    let mut state = self.state.lock().await;
                    // start new batch
                    state.current_slot = Some(transaction_message.slot);
                    state.buffer.push(transaction_message.transaction.clone());
                    info!("started new batch for slot {}", transaction_message.slot);
                }
            }
        }

        // todo: implement logic to flush based on buffer size or time

        Ok(())
    }

    /// flushes any remaining transactions in the buffer, to be used by shutdown sequence
    pub async fn _flush_all(&self) -> Result<()> {
        let batch = {
            let mut state = self.state.lock().await;
            // take the current buffer
            let buffer = std::mem::take(&mut state.buffer);
            let current_slot = state.current_slot;

            BatchState {
                current_slot,
                buffer,
            }
        };

        self.flush(batch).await
    }
}

impl std::fmt::Debug for TransactionWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionWriter")
            .field("config", &self.config)
            .field("failed_log", &"file handle")
            .finish()
    }
}
