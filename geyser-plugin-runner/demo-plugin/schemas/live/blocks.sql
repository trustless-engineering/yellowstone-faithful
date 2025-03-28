-- Latest change to clickhouse schema:
-- --------------------------------
-- Add block_version column:
-- ALTER TABLE test_blocks ADD COLUMN IF NOT EXISTS block_version String DEFAULT '';

DROP TABLE IF EXISTS test_blocks;

CREATE TABLE test_blocks (
    ingest_time DateTime64 DEFAULT toDateTime('1970-01-01 00:00:00', 0),
    -- Corresponds to ingest_time at geyser plugin: DateTime<Utc>
    -- defaults to unix epoch if not set.
    received_time DateTime64 DEFAULT now(),
    -- Corresponds to received_time in clickhouse: DateTime<Utc>
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
    block_time DateTime64 DEFAULT toDateTime('1970-01-01 00:00:00', 0),
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
    block_version String DEFAULT '',
    -- Corresponds to block version: String
    PRIMARY KEY (slot, blockhash) -- Primary key definition
) ENGINE = MergeTree() PARTITION BY toYYYYMM(block_time)
ORDER BY
    (slot, blockhash) SETTINGS index_granularity = 8192;