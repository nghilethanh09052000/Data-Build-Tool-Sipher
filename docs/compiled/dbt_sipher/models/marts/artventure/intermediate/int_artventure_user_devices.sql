WITH raw AS (
    SELECT
        *,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "user_email") AS user_email,
    FROM `sipher-data-testing`.`staging_firebase`.`stg_firebase__artventure_events_all_time`
    WHERE event_name = 'click'
        AND (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "event_label") = 'sign_in_with_google'
)

,device_properties AS (
    SELECT
        user_pseudo_id,
        to_hex(md5(cast(coalesce(cast(device.category as STRING), '') || '-' || coalesce(cast(device.operating_system as STRING), '') as STRING))) AS _device_model_key,
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
    GROUP BY user_pseudo_id, _device_model_key
)

,map_device_to_user_id AS (
    SELECT
        user_id,
        user_email,
        ARRAY_AGG(DISTINCT user_pseudo_id) AS user_pseudo_ids
    FROM raw
    WHERE user_id IS NOT NULL
    GROUP BY user_id, user_email
)

,final AS (
    SELECT
        to_hex(md5(cast(coalesce(cast(user_id as STRING), '') || '-' || coalesce(cast(user_email as STRING), '') || '-' || coalesce(cast(_device_model_key as STRING), '') as STRING))) AS device_sk,
        user_id,
        user_email,
        _device_model_key,
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
    FROM map_device_to_user_id, UNNEST(user_pseudo_ids) AS user_pseudo_id_map
    LEFT JOIN device_properties ON device_properties.user_pseudo_id = user_pseudo_id_map
    GROUP BY user_id, user_email, _device_model_key
)


SELECT
    * EXCEPT(_device_model_key),
    ARRAY[STRUCT(
      CURRENT_TIMESTAMP() AS data_load_timestamp,
      "`sipher-data-testing`.`staging_firebase`.`stg_firebase__artventure_events_all_time`" AS data_sources
    )] AS load_metadata
FROM final