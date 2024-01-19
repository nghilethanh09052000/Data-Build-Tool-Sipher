SELECT  
    DATE(block_timestamp) AS date_tzutc,
    CASE 
            WHEN LOWER(token_address) = '0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511' THEN 'SIPHER Token'
            WHEN LOWER(token_address) = '0x9c57d0278199c931cf149cc769f37bb7847091e7' THEN 'SIPHER INU'
            WHEN LOWER(token_address) = '0x09e0df4ae51111ca27d6b85708cfb3f1f7cae982' THEN 'SIPHER NEKO'
            ELSE 'N/A'
        END AS token_name,
    CASE 
            WHEN LOWER(token_address) = '0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511' THEN CAST(value AS NUMERIC)
            WHEN LOWER(token_address) = '0x9c57d0278199c931cf149cc769f37bb7847091e7' THEN 1
            WHEN LOWER(token_address) = '0x09e0df4ae51111ca27d6b85708cfb3f1f7cae982' THEN 1
            ELSE 0
        END AS sipher_value,
    *
FROM `bigquery-public-data`.`crypto_ethereum`.`token_transfers`
WHERE DATE(block_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
AND LOWER(token_address) IN ('0x9f52c8ecbee10e00d9faaac5ee9ba0ff6550f511', '0x9c57d0278199c931cf149cc769f37bb7847091e7', '0x09e0df4ae51111ca27d6b85708cfb3f1f7cae982')