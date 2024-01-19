WITH raw AS (
  SELECT
    *,
    (SELECT up.value.string_value FROM UNNEST(user_properties) AS up WHERE up.key = "ather_id") AS ather_id,
  FROM `sipher-data-testing`.`staging_firebase`.`stg_firebase__sipher_odyssey_events_all_time`
)
,
geo AS (
  SELECT 
    
  FROM raw
)