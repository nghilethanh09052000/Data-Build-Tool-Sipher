WITH raw AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS date,
        TIMESTAMP_MICROS(event_timestamp) AS timestamp,
        user_id,
        user_pseudo_id,
        event_name,
        event_params
    FROM `sipher-data-testing`.`staging_firebase`.`stg_firebase__artventure_events_all_time`
    WHERE event_name NOT IN (
        'task_registered',
        'task_executing',
        'task_executed'
    )
)
,user_events AS (
    SELECT
        *,
        CASE WHEN 
            (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "page_location") like '%alpha%'
            OR REGEXP_CONTAINS((SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "page_location"), 
                r"https://artventure.ai/ai-recipes/[^/]+")
            THEN 'alpha' ELSE 'internal'
        END AS version
    FROM raw
)

SELECT * FROM user_events