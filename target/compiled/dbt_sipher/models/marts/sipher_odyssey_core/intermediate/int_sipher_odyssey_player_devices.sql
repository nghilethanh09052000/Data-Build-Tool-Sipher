WITH raw AS (
  SELECT
    *,
    (SELECT up.value.string_value FROM UNNEST(user_properties) AS up WHERE up.key = "ather_id") AS ather_id,
    CASE WHEN device.advertising_id IN ('', '00000000-0000-0000-0000-000000000000') THEN NULL ELSE device.advertising_id END AS cleaned_advertising_id
  FROM `sipher-data-testing`.`staging_firebase`.`stg_firebase__sipher_odyssey_events_all_time`
)

,device_properties AS (
  SELECT
    cleaned_advertising_id AS advertising_id,
    device.vendor_id,
    user_pseudo_id,
    to_hex(md5(cast(coalesce(cast(device.category as STRING), '') || '-' || coalesce(cast(device.mobile_model_name as STRING), '') || '-' || coalesce(cast(device.mobile_marketing_name as STRING), '') || '-' || coalesce(cast(device.mobile_os_hardware_model as STRING), '') || '-' || coalesce(cast(device.operating_system as STRING), '') as STRING))) AS _device_model_key,
    MAX(TIMESTAMP_MICROS(event_timestamp)) AS last_used_at,
    MAX(device.mobile_brand_name) AS mobile_brand_name,
    MAX(device.category) AS category,
    MAX(device.mobile_model_name) AS mobile_model_name,
    MAX(device.mobile_marketing_name) AS mobile_marketing_name,
    MAX(device.mobile_os_hardware_model) AS mobile_os_hardware_model,
    MAX(device.operating_system) AS operating_system,
    MAX(device.operating_system_version) AS operating_system_version,
    MAX(device.browser) AS browser,
    MAX(device.browser_version) AS browser_version,
    MAX(device.web_info.browser) AS web_info_browser,
    MAX(device.web_info.browser_version) AS web_info_browser_version,
    MAX(device.web_info.hostname) AS web_info_hostname,
  FROM raw
  GROUP BY advertising_id, vendor_id, user_pseudo_id, _device_model_key
)

,map_device_to_game_user_id AS (
  SELECT
    user_id AS game_user_id,
    ather_id,
    ARRAY_AGG(DISTINCT user_pseudo_id) AS user_pseudo_ids,
  FROM raw
  WHERE user_id IS NOT NULL
  GROUP BY game_user_id, ather_id
)

,final AS (
  SELECT
    to_hex(md5(cast(coalesce(cast(game_user_id as STRING), '') || '-' || coalesce(cast(ather_id as STRING), '') || '-' || coalesce(cast(_device_model_key as STRING), '') as STRING))) AS device_sk,
    game_user_id,
    ather_id,
    _device_model_key,
    ARRAY_AGG(DISTINCT advertising_id IGNORE NULLS) AS advertising_ids,
    ARRAY_AGG(DISTINCT vendor_id IGNORE NULLS) AS vendor_ids,
    ARRAY_AGG(DISTINCT user_pseudo_id IGNORE NULLS) AS user_pseudo_ids,
    MAX(last_used_at) AS last_used_at,
    MAX(category) AS category,
    MAX(mobile_brand_name) AS mobile_brand_name,
    MAX(mobile_model_name) AS mobile_model_name,
    MAX(mobile_marketing_name) AS mobile_marketing_name,
    MAX(mobile_os_hardware_model) AS mobile_os_hardware_model,
    MAX(operating_system) AS operating_system,
    MAX(operating_system_version) AS operating_system_version,
    MAX(browser) AS browser,
    MAX(browser_version) AS browser_version,
    MAX(web_info_browser) AS web_info_browser,
    MAX(web_info_browser_version) AS web_info_browser_version,
    MAX(web_info_hostname) AS web_info_hostname
  FROM map_device_to_game_user_id, UNNEST(user_pseudo_ids) AS user_pseudo_id_map
  LEFT JOIN device_properties ON device_properties.user_pseudo_id = user_pseudo_id_map
  GROUP BY game_user_id, ather_id, _device_model_key
)


,current_ids_and_new_ids AS (
  SELECT
    device_sk,
    advertising_id,
    vendor_id,
    user_pseudo_id
  FROM `sipher-data-testing`.`sipher_odyssey_core`.`int_sipher_odyssey_player_devices`
  LEFT JOIN UNNEST(advertising_ids) AS advertising_id
  LEFT JOIN UNNEST(vendor_ids) AS vendor_id
  LEFT JOIN UNNEST(user_pseudo_ids) AS user_pseudo_id

  UNION DISTINCT

  SELECT
    device_sk,
    advertising_id,
    vendor_id,
    user_pseudo_id
  FROM final
  LEFT JOIN UNNEST(advertising_ids) AS advertising_id
  LEFT JOIN UNNEST(vendor_ids) AS vendor_id
  LEFT JOIN UNNEST(user_pseudo_ids) AS user_pseudo_id
)

,combined_id AS (
  SELECT
    device_sk,
    ARRAY_AGG(DISTINCT advertising_id IGNORE NULLS) AS advertising_ids,
    ARRAY_AGG(DISTINCT vendor_id IGNORE NULLS) AS vendor_ids,
    ARRAY_AGG(DISTINCT user_pseudo_id IGNORE NULLS) AS user_pseudo_ids,
  FROM current_ids_and_new_ids
  GROUP BY device_sk
)

SELECT
  final.*
  EXCEPT(_device_model_key)
  REPLACE (
    combined_id.advertising_ids AS advertising_ids,
    combined_id.vendor_ids AS vendor_ids,
    combined_id.user_pseudo_ids AS user_pseudo_ids
  ),
  ARRAY[STRUCT(
      CURRENT_TIMESTAMP() AS data_load_timestamp,
      "`sipher-data-testing`.`staging_firebase`.`stg_firebase__sipher_odyssey_events_all_time`" AS data_sources
    )] AS load_metadata
FROM final
LEFT JOIN combined_id USING(device_sk)