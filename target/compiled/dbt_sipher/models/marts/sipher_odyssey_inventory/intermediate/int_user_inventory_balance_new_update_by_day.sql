WITH 
instance_all_data_raw AS(
  SELECT DISTINCT
    user_id,
    instance_id,
    item_code,
    item_type,
    item_sub_type,
    rarity,
    MAX(tier) OVER (PARTITION BY user_id, instance_id, updated_balance_timestamp, updated_balance_date) AS tier,
    MAX(level) OVER (PARTITION BY user_id, instance_id, updated_balance_timestamp, updated_balance_date) AS level,
    MAX(ps) OVER (PARTITION BY user_id, instance_id, updated_balance_timestamp, updated_balance_date) AS ps,
    boost,
    MAX(updated_balance) OVER (PARTITION BY user_id, instance_id, updated_balance_timestamp, updated_balance_date) AS updated_balance,
    updated_balance_date,
    updated_balance_timestamp
  FROM `sipher-data-testing`.`staging_sipher_server`.`stg_sipher_server__raw_inventory_balancing_update` 
  -- WHERE updated_balance_date <= '2023-12-25'
  WHERE updated_balance_date BETWEEN DATE_ADD('', INTERVAL -3 DAY) AND ''
  ORDER BY item_type, instance_id
  )

, instance_all_data AS(
  SELECT DISTINCT *
  FROM instance_all_data_raw
)

  SELECT 
    instance_all_data.* EXCEPT(updated_balance_timestamp),
    TIMESTAMP_MICROS(updated_balance_timestamp) AS updated_balance_timestamp
  FROM instance_all_data