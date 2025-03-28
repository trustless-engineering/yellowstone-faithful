--DROP TABLE IF EXISTS txs;
CREATE TABLE txs (
    signature String,
    is_vote Boolean,
    `index` Nullable(UInt64),
    num_required_signatures UInt8,
    num_readonly_signed_accounts UInt8,
    num_readonly_unsigned_accounts UInt8,
    account_keys Array(String),
    recent_blockhash String,
    instructions Array(
        Tuple(
            program_id_index UInt8,
            accounts Array(String),
            data String
        )
    ),
    success Bool,
    error Nullable(String),
    fee Int64,
    pre_balances Array(Int64),
    post_balances Array(Int64),
    inner_instructions Array(
        Tuple(
            accounts Array(String),
            data String
        )
    ),
    log_messages Array(String),
    pre_token_balances Array(
        Tuple(
            account String,
            mint String,
            owner String,
            amount Decimal(38, 18)
        )
    ),
    post_token_balances Array(
        Tuple(
            account String,
            mint String,
            owner String,
            amount Decimal(38, 18)
        )
    ),
    rewards Array(
        Tuple(
            pubkey String,
            lamports Int64,
            post_balance Int64,
            reward_type Nullable(Enum8(
                'Fee' = 1,
                'Rent' = 2,
                'Staking' = 3,
                'Voting' = 4
            )),
            commission Nullable(UInt8)
        )
    ),
    loaded_addresses Array(String),
    return_data Tuple(
        program_id String,
        data String
    ),
    compute_units_consumed Int64,
    signatures Array(String),
    signers Array(String),
    PRIMARY KEY signature
) ENGINE = MergeTree() SETTINGS index_granularity = 8192;

-- Geyser structs
-- #[repr(C)]
-- pub struct ReplicaTransactionInfo<'a> {
--     pub signature: &'a Signature,
--     pub is_vote: bool,
--     pub transaction: &'a SanitizedTransaction,
--     pub transaction_status_meta: &'a TransactionStatusMeta,
-- }
-- #[repr(C)]
-- pub struct ReplicaTransactionInfoV2<'a> {
--     pub signature: &'a Signature,
--     pub is_vote: bool,
--     pub transaction: &'a SanitizedTransaction,
--     pub transaction_status_meta: &'a TransactionStatusMeta,
--     pub index: usize,
-- }
-- pub struct TransactionStatusMeta {
--     pub status: Result<()>,
--     pub fee: u64,
--     pub pre_balances: Vec<u64>,
--     pub post_balances: Vec<u64>,
--     pub inner_instructions: Option<Vec<InnerInstructions>>,
--     pub log_messages: Option<Vec<String>>,
--     pub pre_token_balances: Option<Vec<TransactionTokenBalance>>,
--     pub post_token_balances: Option<Vec<TransactionTokenBalance>>,
--     pub rewards: Option<Rewards>,
--     pub loaded_addresses: LoadedAddresses,
--     pub return_data: Option<TransactionReturnData>,
--     pub compute_units_consumed: Option<u64>,
-- }
-- pub struct Transaction {
--     pub signatures: Vec<Signature>,
--     pub message: Message,
-- }
-- pub struct VersionedTransaction {
--     pub signatures: Vec<Signature>,
--     pub message: VersionedMessage,
-- }
-- pub enum VersionedMessage {
--     Legacy(Message),
--     V0(Message),
-- }
-- pub struct Message {
--     pub header: MessageHeader,
--     pub account_keys: Vec<Pubkey>,
--     pub recent_blockhash: Hash,
--     pub instructions: Vec<CompiledInstruction>,
-- }
-- pub struct MessageHeader {
--     pub num_required_signatures: u8,
--     pub num_readonly_signed_accounts: u8,
--     pub num_readonly_unsigned_accounts: u8,
-- }
-- pub struct CompiledInstruction {
--     pub program_id_index: u8,
--     pub accounts: Vec<u8>,
--     pub data: Vec<u8>,
-- }
-- pub struct TransactionReturnData {
--     pub program_id: Pubkey,
--     pub data: Vec<u8>,
-- }
---