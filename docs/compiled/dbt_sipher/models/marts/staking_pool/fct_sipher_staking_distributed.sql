WITH distributed_tx_hash AS(
   SELECT  
    DISTINCT transaction_hash
   FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
   WHERE DATE(block_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND ( 
         LOWER(from_address) = '0x7776c65e112475cebd4a2fc72e685f35641db3da'
         OR LOWER(to_address) = '0x7776c65e112475cebd4a2fc72e685f35641db3da'
        )
   )

   ,filtered_distributed_tx AS(
      SELECT
         DATE(block_timestamp) AS act_date,
         token_address,
         CASE
                  WHEN LOWER(token_address) = '0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511' THEN 'SIPHER Token'
                  WHEN LOWER(token_address) = '0xb2d1464ae4cc86856474a34d112b4a2efa326ed9' THEN 'Escrowed SIPHER Token'
                  ELSE 'N/A'
            END AS token_name,
          CAST(value AS NUMERIC) AS sipher_value,
         * EXCEPT(value, token_address)
      FROM `bigquery-public-data.crypto_ethereum.token_transfers`
      WHERE transaction_hash IN (SELECT * FROM distributed_tx_hash)
   )

   SELECT * FROM filtered_distributed_tx