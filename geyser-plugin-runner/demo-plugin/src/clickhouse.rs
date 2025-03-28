use crate::config::ClickHouseConfig;
use anyhow::Result;
use clickhouse::{Client, Row};
use serde::Serialize;
use serde_with::serde_as;
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaBlockInfoVersions, ReplicaTransactionInfoVersions,
};
use solana_sdk::bs58;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{RewardType, TransactionTokenBalance};

#[serde_as]
#[derive(Row, Serialize, Debug)]
pub struct BlockRow {
    slot: u64,
    blockhash: String,
    rewards: Vec<(String, i64, u64, Option<u8>, Option<u8>)>,
    block_time: Option<i64>,
    block_height: Option<u64>,
    parent_slot: Option<u64>,
    parent_blockhash: Option<String>,
    executed_transaction_count: Option<u64>,
    entry_count: Option<u64>,
}

impl BlockRow {
    fn from_replica(replica: ReplicaBlockInfoVersions) -> Self {
        let (
            slot,
            blockhash,
            rewards,
            block_time,
            block_height,
            parent_slot,
            parent_blockhash,
            executed_transaction_count,
            entry_count,
        ) = match replica {
            ReplicaBlockInfoVersions::V0_0_1(info) => (
                info.slot,
                info.blockhash,
                info.rewards,
                info.block_time,
                info.block_height,
                None,
                None,
                None,
                None,
            ),
            ReplicaBlockInfoVersions::V0_0_2(info) => (
                info.slot,
                info.blockhash,
                info.rewards,
                info.block_time,
                info.block_height,
                Some(info.parent_slot),
                Some(info.parent_blockhash),
                Some(info.executed_transaction_count),
                None,
            ),
            ReplicaBlockInfoVersions::V0_0_3(info) => (
                info.slot,
                info.blockhash,
                info.rewards,
                info.block_time,
                info.block_height,
                Some(info.parent_slot),
                Some(info.parent_blockhash),
                Some(info.executed_transaction_count),
                Some(info.entry_count),
            ),
        };

        // Log blockhash and slot for debugging
        // println!(
        //     "Blockhash and slot: Slot: {}, Blockhash: {}, Blocktime: {:?}",
        //     slot, blockhash, block_time
        // );

        BlockRow {
            slot,
            blockhash: blockhash.to_string(),
            rewards: rewards
                .iter()
                .map(|r| {
                    (
                        r.pubkey.to_string(),
                        r.lamports,
                        r.post_balance,
                        r.reward_type.map(|rt| match rt {
                            RewardType::Fee => 1,
                            RewardType::Rent => 2,
                            RewardType::Staking => 3,
                            RewardType::Voting => 4,
                        }),
                        r.commission,
                    )
                })
                .collect(),
            block_time,
            block_height,
            parent_slot,
            parent_blockhash: parent_blockhash.map(|s| s.to_string()),
            executed_transaction_count,
            entry_count,
        }
    }
}

#[serde_as]
#[derive(Row, Serialize, Debug)]
struct TransactionRow {
    signature: String,
    is_vote: u8,
    index: Option<u64>,
    num_required_signatures: u8,
    num_readonly_signed_accounts: u8,
    num_readonly_unsigned_accounts: u8,
    account_keys: Vec<String>,
    recent_blockhash: String,
    instructions: Vec<(u8, Vec<String>, String)>,
    success: bool,
    error: Option<String>,
    fee: i64,
    pre_balances: Vec<i64>,
    post_balances: Vec<i64>,
    inner_instructions: Vec<(Vec<String>, String)>,
    log_messages: Vec<String>,
    pre_token_balances: Vec<(String, String, String, String)>,
    post_token_balances: Vec<(String, String, String, String)>,
    rewards: Vec<(String, i64, u64, Option<u8>, Option<u8>)>,
    loaded_addresses: Vec<String>,
    return_data: (String, String),
    compute_units_consumed: i64,
    signatures: Vec<String>,
    signers: Vec<String>,
}

impl TransactionRow {
    fn from_replica(replica: ReplicaTransactionInfoVersions) -> Self {
        let (signature, is_vote, tx, meta, index) = match replica {
            ReplicaTransactionInfoVersions::V0_0_1(info) => (
                info.signature.to_string(),
                info.is_vote,
                info.transaction.clone(),
                info.transaction_status_meta.clone(),
                None,
            ),
            ReplicaTransactionInfoVersions::V0_0_2(info) => (
                info.signature.to_string(),
                info.is_vote,
                info.transaction.clone(),
                info.transaction_status_meta.clone(),
                Some(info.index),
            ),
        };

        // println!("message_hash: {:?}", hash);
        let message = tx.message();

        let mut all_signers: Vec<Pubkey> = Vec::new();
        for (ix_index, _ix) in message.instructions().iter().enumerate() {
            let signers = message.get_ix_signers(ix_index);
            all_signers.extend(signers);
        }

        let token_balance_to_tuple = |tb: &TransactionTokenBalance| {
            (
                tb.account_index.to_string(),
                tb.mint.clone(),
                tb.owner.clone(),
                tb.ui_token_amount.amount.clone(),
            )
        };

        // Log signatures for debugging
        // println!("Transaction signatures:");
        // for (index, signature) in tx.signatures().iter().enumerate() {
        //     println!("Signature {}: {}", index, signature);
        // }

        TransactionRow {
            signature,
            is_vote: if is_vote { 1 } else { 0 },
            index: index.map(|i| i as u64),
            num_required_signatures: message.header().num_required_signatures,
            num_readonly_signed_accounts: message.header().num_readonly_signed_accounts,
            num_readonly_unsigned_accounts: message.header().num_readonly_unsigned_accounts,
            account_keys: message
                .account_keys()
                .iter()
                .map(|k| k.to_string())
                .collect(),
            recent_blockhash: message.recent_blockhash().to_string(),
            instructions: message
                .instructions()
                .iter()
                .map(|i| {
                    (
                        i.program_id_index,
                        i.accounts
                            .iter()
                            .map(|&index| message.account_keys()[index as usize].to_string())
                            .collect::<Vec<String>>(),
                        bs58::encode(&i.data).into_string(),
                    )
                })
                .collect(),
            success: meta.status.is_ok(),
            error: meta.status.err().map(|e| e.to_string()),
            fee: meta.fee as i64,
            pre_balances: meta.pre_balances.iter().map(|&b| b as i64).collect(),
            post_balances: meta.post_balances.iter().map(|&b| b as i64).collect(),
            inner_instructions: meta
                .inner_instructions
                .unwrap_or_default()
                .into_iter()
                .flat_map(|ii| {
                    ii.instructions
                        .into_iter()
                        .map(|i| {
                            (
                                i.instruction
                                    .accounts
                                    .iter()
                                    .map(|&index| {
                                        message.account_keys()[index as usize].to_string()
                                    })
                                    .collect::<Vec<String>>(),
                                bs58::encode(&i.instruction.data).into_string(),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect(),
            log_messages: meta.log_messages.unwrap_or_default(),
            pre_token_balances: meta
                .pre_token_balances
                .unwrap_or_default()
                .iter()
                .map(token_balance_to_tuple)
                .collect(),
            post_token_balances: meta
                .post_token_balances
                .unwrap_or_default()
                .iter()
                .map(token_balance_to_tuple)
                .collect(),
            rewards: meta
                .rewards
                .unwrap_or_default()
                .iter()
                .map(|r| {
                    (
                        r.pubkey.to_string(),
                        r.lamports,
                        r.post_balance,
                        r.reward_type.map(|rt| match rt {
                            RewardType::Fee => 1,
                            RewardType::Rent => 2,
                            RewardType::Staking => 3,
                            RewardType::Voting => 4,
                        }),
                        r.commission,
                    )
                })
                .collect(),
            loaded_addresses: meta
                .loaded_addresses
                .writable
                .iter()
                .chain(meta.loaded_addresses.readonly.iter())
                .map(|addr| addr.to_string())
                .collect(),
            return_data: meta
                .return_data
                .map(|rd| {
                    (
                        rd.program_id.to_string(),
                        bs58::encode(&rd.data).into_string(),
                    )
                })
                .unwrap_or_default(),
            compute_units_consumed: meta.compute_units_consumed.unwrap_or_default() as i64,
            signatures: tx.signatures().iter().map(|s| s.to_string()).collect(),
            signers: all_signers.iter().map(|s| s.to_string()).collect(),
        }
    }
}

// Define the ClickHouseWriter struct
pub struct ClickHouseWriter {
    client: Client,
    config: ClickHouseConfig,  // Add this line
    block_buffer: Vec<BlockRow>,
    transaction_buffer: Vec<TransactionRow>,
    buffer_limit: usize,
    total_blocks: usize,
    total_transactions: usize,
}

impl ClickHouseWriter {
    pub fn new(config: ClickHouseConfig) -> Result<Self> {
        let client = Client::default()
            .with_url(&config.url)
            .with_database(&config.database)
            .with_user(&config.username)
            .with_password(&config.password);
        Ok(Self {
            client,
            buffer_limit: config.batch_size,
            config, 
            block_buffer: Vec::new(),
            transaction_buffer: Vec::new(),
            total_blocks: 0,
            total_transactions: 0,
        })
    }

    pub async fn add_blocks(&mut self, blocks: Vec<ReplicaBlockInfoVersions<'_>>) -> Result<()> {
        for replica in blocks {
            let row = BlockRow::from_replica(replica);
            self.block_buffer.push(row);
            self.total_blocks += 1;
        }

        if self.block_buffer.len() >= self.buffer_limit {
            self.flush_blocks().await?;
        }

        Ok(())
    }

    pub async fn flush_blocks(&mut self) -> Result<()> {
        if self.block_buffer.is_empty() {
            return Ok(());
        }

        let len = self.block_buffer.len();
        println!("Flushing {} blocks to ClickHouse", len);

        let mut insert = self.client.insert(&self.config.blocks_table).unwrap();
        for row in self.block_buffer.drain(..) {
            insert.write(&row).await.expect("Failed to write block row");
        }
        insert.end().await.expect("Failed to end block insert");
        println!(
            "Flushed {} blocks, total blocks: {}",
            len, self.total_blocks
        );
        Ok(())
    }

    pub async fn send_blocks(&mut self) -> Result<()> {
        self.flush_blocks().await
    }

    pub async fn add_transactions(
        &mut self,
        transactions: Vec<ReplicaTransactionInfoVersions<'_>>,
        _slot: u64,
    ) -> Result<()> {
        for replica in transactions {
            let row = TransactionRow::from_replica(replica);
            self.transaction_buffer.push(row);
            self.total_transactions += 1;
        }

        if self.transaction_buffer.len() >= self.buffer_limit {
            self.flush_transactions().await?;
        }

        Ok(())
    }

    async fn flush_transactions(&mut self) -> Result<()> {
        if self.transaction_buffer.is_empty() {
            return Ok(());
        }

        let len = self.transaction_buffer.len();
        println!("Flushing {} transactions to ClickHouse", len);

        let mut insert = self.client.insert(&self.config.transactions_table).unwrap();
        for row in self.transaction_buffer.drain(..) {
            insert.write(&row).await.expect("Failed to write transaction row");
        }
        insert.end().await.expect("Failed to end transaction insert");
        println!(
            "Flushed {} transactions, total transactions: {}",
            len, self.total_transactions
        );
        Ok(())
    }

    pub async fn send_transactions(&mut self) -> Result<()> {
        self.flush_transactions().await
    }
}
