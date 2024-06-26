SELECT
    tx_type,
    token_type,
    pool_type,
    transaction_hash,
    from_address,
    to_address,
    hash_count,
    DATE(block_timestamp) AS act_date,
    SUM(CAST(value AS FLOAT64)*POWER(10,-18)) AS staked_value,
  FROM `sipher-data-testing`.`staking_public_data`.`fct_staking_pool_transaction`
  GROUP BY 1,2,3,4,5,6,7,8