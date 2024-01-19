WITH fct AS (
        SELECT
            PARSE_DATE('%Y%m%d', event_date)                                    AS date
            ,TIMESTAMP_MICROS(event_timestamp)                                  AS ts
            ,(SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "task_id")            AS task_id
            ,event_name                                                         AS status
            ,(SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "execution_duration")    AS exc_duration
            ,(SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "runpod_queue_duration") AS runpod_queue_duration
            ,(SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "queue_duration")        AS queue_duration
        FROM 
            `sipher-data-testing`.`staging_firebase`.`stg_firebase__artventure_events_all_time`
        WHERE 
            event_name IN ('task_registered', 'task_executing', 'task_executed')
    )
    ,task_executing AS (
        SELECT
            *
        FROM fct
        WHERE task_id NOT IN (
            SELECT
                DISTINCT task_id
            FROM fct 
            WHERE status = 'task_executed'
            ) 
            AND status = 'task_executing'
    )
SELECT * 
FROM fct
WHERE 
    status IN ('task_registered', 'task_executed')

UNION ALL

SELECT * 
FROM task_executing