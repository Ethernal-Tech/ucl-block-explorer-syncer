TRUNCATE chain.blocks, chain.transactions, chain.metadata;

SELECT
    LEFT(hash, 10) AS hash,
    number,
    LEFT(parent_hash, 10) AS parent_hash,
    nonce,
    LEFT(sha3_uncles, 10) AS sha3_uncles,
    LEFT(logs_bloom::text, 10) AS logs_bloom,
    LEFT(transactions_root, 10) AS transactions_root,
    LEFT(state_root, 10) AS state_root,
    LEFT(receipts_root, 10) AS receipts_root,
    LEFT(miner, 10) AS miner,
    difficulty,
    total_difficulty,
    LEFT(extra_data, 10) AS extra_data,
    size,
    gas_limit,
    gas_used,
    timestamp,
    LEFT(mix_hash, 10) AS mix_hash,
    base_fee,
    txn_count
FROM chain.blocks
ORDER BY number ASC;

SELECT
    hash,
    LEFT(block_hash, 10) AS block_hash,
    block_number,
    block_timestamp,
    LEFT(from_address, 10) AS from_address,
    LEFT(to_address, 10) AS to_address,
    value,
    nonce,
    gas_limit,
    gas_price,
    gas_fee_cap,
    gas_tip_cap,
    LEFT(data, 10) AS data,
    data_method,
    type,
    chain_id,
    status,
    created_at,
    updated_at
FROM chain.transactions
ORDER BY block_number ASC;