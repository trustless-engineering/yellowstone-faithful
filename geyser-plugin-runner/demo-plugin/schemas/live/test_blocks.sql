DROP TABLE IF EXISTS test_blocks;

CREATE TABLE test_blocks (
    ingest_time DateTime64 DEFAULT toDateTime('1970-01-01 00:00:00', 0),
    -- Corresponds to ingest_time at geyser plugin: DateTime<Utc>
    -- defaults to unix epoch if not set.
    received_time DateTime64 DEFAULT now(),
    -- Corresponds to received_time in clickhouse: DateTime<Utc>
    -- defaults to now and should not be sent be client
    slot UInt64,
    -- Corresponds to slot: u64
    blockhash String,
    -- Corresponds to blockhash: String
    rewards_count UInt64,
    -- Change from UInt32 to UInt64 to match usize
    reward_pubkey String DEFAULT '',
    -- Corresponds to reward_pubkey: String (default to empty string)
    reward_lamports Int64,
    -- Change from UInt64 to Int64 to match i64
    reward_post_balance UInt64,
    -- Corresponds to reward_post_balance: u64
    reward_type UInt8,
    -- Corresponds to reward_type: u8
    reward_commission UInt8,
    -- Corresponds to reward_commission: u8
    block_time DateTime64 DEFAULT toDateTime64('1970-01-01 00:00:00', 0),
    -- Change to DateTime64
    block_height UInt64,
    -- Corresponds to block_height: u64
    parent_slot UInt64,
    -- Corresponds to parent_slot: u64
    parent_blockhash String DEFAULT '',
    -- Corresponds to parent_blockhash: String (default to empty string)
    executed_transaction_count UInt64,
    -- Corresponds to executed_transaction_count: u64
    entry_count UInt64,
    -- Corresponds to entry_count: u64
    PRIMARY KEY (slot, blockhash) -- Primary key definition
) ENGINE = MergeTree() PARTITION BY toYYYYMM(block_time)
ORDER BY
    (slot, blockhash) SETTINGS index_granularity = 8192;

--- Geyser structs
-- pub enum RewardType {
--     Fee,
--     Rent,
--     Staking,
--     Voting,
-- }
-- pub struct Reward {
--     pub pubkey: String,
--     pub lamports: i64,
--     pub post_balance: u64,
--     pub reward_type: Option<RewardType>,
--     pub commission: Option<u8>,
-- }
-- pub struct ReplicaBlockInfo<'a> {
--     pub slot: Slot,
--     pub blockhash: &'a str,
--     pub rewards: &'a [Reward],
--     pub block_time: Option<UnixTimestamp>,
--     pub block_height: Option<u64>,
-- }
-- pub struct ReplicaBlockInfoV2<'a> {
--     pub slot: Slot,
--     pub blockhash: &'a str,
--     pub rewards: &'a [Reward],
--     pub block_time: Option<UnixTimestamp>,
--     pub block_height: Option<u64>,
--     pub parent_slot: Slot,
--     pub parent_blockhash: &'a str,
--     pub executed_transaction_count: u64,
-- }
-- pub struct ReplicaBlockInfoV3<'a> {
--     pub parent_slot: Slot,
--     pub parent_blockhash: &'a str,
--     pub slot: Slot,
--     pub blockhash: &'a str,
--     pub rewards: &'a [Reward],
--     pub block_time: Option<UnixTimestamp>,
--     pub block_height: Option<u64>,
--     pub executed_transaction_count: u64,
--     pub entry_count: u64,
-- }