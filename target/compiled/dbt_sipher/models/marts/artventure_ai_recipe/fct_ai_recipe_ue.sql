WITH ue AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS date
        ,user_id
        ,event_name
        ,event_params
        , (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "page_location") AS page_location 
    FROM 
        `sipher-data-testing`.`staging_firebase`.`stg_firebase__artventure_events_all_time`
    WHERE 
        REGEXP_CONTAINS(
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
        AND event_name IN ('user_capacity', 'login', 'form_start', 'click')
    ORDER BY 
        TIMESTAMP_MICROS(event_timestamp) DESC
)

SELECT 
    * EXCEPT(page_location)
    ,COALESCE(REGEXP_EXTRACT(page_location, r'[?&]recipe=([^&]+)'), REGEXP_EXTRACT(page_location, r'/alpha/([^?]+)')) AS recipe 
FROM 
    ue
WHERE 
    user_id IS NOT NULL