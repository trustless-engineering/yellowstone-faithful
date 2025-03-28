DROP VIEW IF EXISTS transactions_mv;

CREATE MATERIALIZED VIEW transactions_mv ENGINE = MergeTree()
ORDER BY
    (slot) POPULATE AS
SELECT
    blocks.block_time as `time`,
    toDate(blocks.block_time) as date,
    blocks.slot as slot,
    transactions.index as index,
    transactions.fee as fee,
    transactions.num_required_signatures as required_signatures,
    transactions.num_readonly_signed_accounts as readonly_signed_accounts,
    transactions.num_readonly_unsigned_accounts as readonly_unsigned_accounts,
    transactions.recent_blockhash as block_hash,
    transactions.signature as id,
    transactions.signature as signature,
    transactions.success as success,
    transactions.error as err,
    transactions.recent_blockhash as recent_block_hash,
    transactions.instructions as instructions,
    transactions.account_keys as account_keys,
    transactions.log_messages as log_messages,
    transactions.pre_balances as pre_balances,
    transactions.post_balances as post_balances,
    transactions.pre_token_balances as pre_token_balances,
    transactions.post_token_balances as post_token_balances,
    transactions.signatures as signatures,
    transactions.signers as signers
FROM
    transactions
    LEFT JOIN blocks ON transactions.recent_blockhash = blocks.blockhash
WHERE
    blocks.block_time IS NOT NULL
    AND blocks.slot IS NOT NULL;