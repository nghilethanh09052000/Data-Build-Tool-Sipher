WITH raw_internal AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date)                                             AS act_date
    ,TIMESTAMP_MICROS(event_timestamp)                                           AS ts
    ,(SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "event_label")                 AS event_label   
    ,*
  FROM 
    `sipher-data-testing`.`staging_firebase`.`stg_firebase__artventure_events_all_time`
  WHERE 
    NOT REGEXP_CONTAINS(
    (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "page_location"), r'https://artventure\.ai/alpha/[^/]+')
    OR 
      (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "page_location")
      NOT IN (
      'https://artventure.ai/ai-recipes?recipe=art-toy'
      ,'https://artventure.ai/ai-recipes?recipe=anime'
      ,'https://artventure.ai/ai-recipes?recipe=ai-qr-code'
      ,'https://artventure.ai/ai-recipes?recipe=commercial-photoshoot'
      ,'https://artventure.ai/ai-recipes?recipe=logo-art'
    )
    AND 
      event_name IN ('click')
    AND 
      (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "event_label") = 'recipe-generate'
)
,raw_events_generate AS (
  SELECT
    act_date
    ,ts
    ,user_id
    ,geo.country AS country
    ,platform
    ,COALESCE(
      (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "recipe_name") 
      , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "recipe_id")
    ) AS recipe
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "template")             AS template
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "theme")                AS theme
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "backgroundColor")      AS background_color
    , (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "shapePrecisionLevel")     AS shape_precision_level
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "style")                AS style
    ,COALESCE(
      (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "creative_intensity")
      , (SELECT ep.value.double_value FROM UNNEST(event_params) AS ep WHERE ep.key = "creative_intensity")
      , (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "creativeIntensity")
      , (SELECT ep.value.double_value FROM UNNEST(event_params) AS ep WHERE ep.key = "creativeIntensity")
    )   AS creative_intensity
    ,COALESCE(
      (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "similarity")
      , (SELECT ep.value.double_value FROM UNNEST(event_params) AS ep WHERE ep.key = "similarity")
    )   AS similarity
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "prompt")             AS prompt
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "negative_prompt")    AS negative_prompt
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "aspectRatio")        AS aspect_ratio
    , (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "upscaleRatio")          AS upscale_ratio
    , (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "denoising_strength")    AS denoising_strength
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "shapeControl")       AS shape_control
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "modelHash")          AS model_hash
    , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "pose")               AS pose
    , (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "duration")              AS duration
  FROM 
    raw_internal
  WHERE 
    event_label = 'recipe-generate'
)

SELECT * 
FROM 
  raw_events_generate