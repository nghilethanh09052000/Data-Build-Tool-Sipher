WITH raw AS (
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS date,
        TIMESTAMP_MICROS(event_timestamp) AS timestamp,
        event_name,
        event_params
    FROM `sipher-data-testing`.`staging_firebase`.`stg_firebase__artventure_events_all_time`
    WHERE event_name IN (
        'task_registered',
        'task_executing',
        'task_executed'
    )
)
,task_events AS (
    SELECT
        date,
        timestamp,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "task_id") AS task_id,
        event_name AS status,
        (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "tasks_in_queue") AS tasks_in_queue,
        (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "queue_duration") AS queue_duration,
        (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "runpod_queue_duration") AS runpod_queue_duration,
        (SELECT ep.value.int_value FROM UNNEST(event_params) AS ep WHERE ep.key = "execution_duration") AS execution_duration
    FROM raw
)

SELECT * FROM task_events