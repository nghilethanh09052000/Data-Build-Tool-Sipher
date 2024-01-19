WITH raw_alpha AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date)  AS act_date
    ,TIMESTAMP_MICROS(event_timestamp) AS ts
    ,*
  FROM 
    `sipher-data-testing`.`staging_firebase`.`stg_firebase__artventure_events_all_time`
  WHERE 
    REGEXP_CONTAINS (
    (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "page_location")
    , r'https://artventure\.ai/alpha/[^/]+'
    )
    OR (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "page_location") IN (
      'https://artventure.ai/ai-recipes?recipe=art-toy'
      ,'https://artventure.ai/ai-recipes?recipe=anime'
      ,'https://artventure.ai/ai-recipes?recipe=ai-qr-code'
      ,'https://artventure.ai/ai-recipes?recipe=commercial-photoshoot'
      ,'https://artventure.ai/ai-recipes?recipe=logo-art'
    )
    AND event_name IN ('click')
    AND (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "event_label") IN ('recipe_alpha_generate', 'feedback')
)
,raw_events AS (
  SELECT
    act_date
    ,ts
    ,user_id
    ,geo.country AS country
    ,platform
    ,event_name
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "event_category") AS event_category
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "event_label")    AS event_label
    ,event_params
  FROM 
    raw_alpha
)
,generate_and_feedback AS (
  SELECT
    act_date
    ,ts
    ,user_id
    ,country
    ,platform
    ,event_label AS event_name
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "prompt")    AS prompt
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "style")     AS style
    , COALESCE(
      (SELECT ep.value.double_value FROM UNNEST(event_params) AS ep WHERE ep.key = "creative_intensity")
      , (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "creative_intensity")
    )           AS creative_intensity
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "template")  AS template
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "response")  AS response
    , COALESCE(
      (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "recipe_id")
      ,'anime'
    )           AS recipe_id
  FROM 
    raw_events
)
,cohort_date AS (
  SELECT
    user_id
    ,country
    ,platform
    ,event_name
    ,MIN(ts)       AS cohort_ts
    ,MIN(DATE(ts)) AS cohort_date
  FROM 
    generate_and_feedback
  GROUP BY 1,2,3,4
)

SELECT
  c.cohort_ts
  ,c.cohort_date
  ,DATE_DIFF(g.act_date, c.cohort_date, DAY) AS day_diff
  ,g.*
FROM 
  cohort_date c 
  LEFT JOIN generate_and_feedback g 
  USING(user_id, country, platform, event_name)