WITH raw AS (
  SELECT
    PARSE_DATE('%Y%m%d', MIN(event_date))        AS first_date_signin
    ,user_id
    ,device.category                             AS device_category
    ,device.mobile_brand_name                    AS mobile_brand_name
    ,device.mobile_model_name                    AS mobile_model_name
    ,device.operating_system                     AS os
    ,device.browser                              AS browser
    ,geo.continent                               AS continent
    ,geo.country                                 AS country
    ,geo.city                                    AS city
    ,traffic_source.name                         AS traffic_name
    ,traffic_source.medium                       AS traffic_medium
    ,traffic_source.source                       AS traffic_source
    ,(SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "page_location")   AS page_location
  FROM 
      `sipher-data-testing`.`staging_firebase`.`stg_firebase__artventure_events_all_time`
  WHERE 
    event_name = 'login' 
    AND user_id IS NOT NULL
    AND REGEXP_CONTAINS(
      (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "page_location")
      , r'https://artventure\.ai/alpha/[^/]+')
    OR 
      (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "page_location") IN (
        'https://artventure.ai/ai-recipes?recipe=art-toy'
        ,'https://artventure.ai/ai-recipes?recipe=anime'
        ,'https://artventure.ai/ai-recipes?recipe=ai-qr-code'
        ,'https://artventure.ai/ai-recipes?recipe=commercial-photoshoot'
        ,'https://artventure.ai/ai-recipes?recipe=logo-art'
    ) 
  GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13,14
)
,final AS (
  SELECT
    * EXCEPT(page_location)
    , COALESCE(REGEXP_EXTRACT(page_location, r'[?&]recipe=([^&]+)'), REGEXP_EXTRACT(page_location, r'/alpha/([^?]+)')) AS recipe
  FROM 
      raw
)
SELECT * 
FROM 
  final