WITH fact AS (
    SELECT
        *
    FROM `sipher-data-testing`.`tmp_dbt`.`fct_artventure_task_events`
)
,reporting AS (
    SELECT
        date,SUM(CASE WHEN status = 'task_registered' THEN 1 ELSE 0 END) AS task_registered,
        SUM(CASE WHEN status = 'task_executing' THEN 1 ELSE 0 END) AS task_executing,
        SUM(CASE WHEN status = 'task_executed' THEN 1 ELSE 0 END) AS task_executed,
        AVG(tasks_in_queue) / 1000 AS avg_tasks_in_queue,
        AVG(queue_duration) / 1000 AS avg_queue_duration,
        AVG(runpod_queue_duration) / 1000 AS avg_runpod_queue_duration,
        AVG(execution_duration) / 1000 AS avg_execution_duration
    FROM fact
    GROUP BY date
)

SELECT * FROM reporting