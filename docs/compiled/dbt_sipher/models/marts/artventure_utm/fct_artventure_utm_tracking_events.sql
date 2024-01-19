WITH raw AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date)         AS event_date
    ,TIMESTAMP_MICROS(event_timestamp)       AS ts
    ,CASE
      WHEN user_id='anonymous' THEN NULL
      ELSE user_id
    END                        AS user_id
    ,user_pseudo_id
    ,event_name
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "page_location")        AS page_location
    , (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "ga_session_id")           AS ga_session_id
    , (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "engagement_time_msec")    AS engagement_time_msec
    ,event_params
  FROM 
    `sipher-data-testing`.`staging_firebase`.`stg_firebase__artventure_events_all_time`
  WHERE 
    (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "page_location") LIKE "%utm%"
    AND 
      event_name NOT IN ('user_engagement')
)
SELECT
  event_date
  ,ts
  ,user_id
  ,user_pseudo_id
  ,event_name
  ,event_params
  ,ga_session_id
  ,COUNT(DISTINCT ga_session_id) OVER (PARTITION BY user_pseudo_id)                           AS session_cnt
  ,(SUM(engagement_time_msec)    OVER (PARTITION BY ga_session_id, user_pseudo_id))  /1000    AS session_time_sec
  ,COALESCE(REGEXP_EXTRACT(page_location, r'/alpha/([^?]+)\?'), 'artventure')                 AS recipe
  ,REGEXP_EXTRACT(page_location, 'utm_source=([^&]+)')       AS source
  ,REGEXP_EXTRACT(page_location, 'utm_medium=([^&]+)')       AS medium
  ,REGEXP_EXTRACT(page_location, 'utm_campaign=([^&]+)')     AS campaign
FROM 
  raw
ORDER BY 
  user_pseudo_id, ts