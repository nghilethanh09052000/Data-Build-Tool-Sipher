WITH fact AS (
    SELECT
        *
    FROM 
        `sipher-data-testing`.`artventure_ai_recipe`.`fct_ai_recipe_task_be`
),
total_tasks AS (
    SELECT
        date
        ,SUM(CASE WHEN status = 'task_registered' THEN 1 ELSE 0 END) AS cnt_task_registered
        ,SUM(CASE WHEN status = 'task_executing'  THEN 1 ELSE 0 END) AS cnt_task_executing
        ,SUM(CASE WHEN status = 'task_executed'   THEN 1 ELSE 0 END) AS cnt_task_executed
    FROM 
        fact
    GROUP BY 1
),
avg_ex_time AS (
    SELECT
        date
        ,AVG(exc_duration / 1000000) AS avg_execution_time
    FROM (
        SELECT
            date
            ,task_id
            ,exc_duration   
        FROM fact
        WHERE status = 'task_executed'
    ) s
    GROUP BY 1
),
avg_pending_time AS (
    SELECT
        date
        ,AVG(pending_time) AS avg_pending_time
    FROM (
        SELECT
            date
            ,TIMESTAMP_DIFF(MAX(ts), MIN(ts), SECOND) / 60 AS pending_time
        FROM (
            SELECT
                date
                ,ts
                ,task_id
                ,RANK() OVER(PARTITION BY ts, task_id ORDER BY ts DESC) AS rank_ts_task
            FROM 
                fact
        )
        WHERE rank_ts_task <= 2
        GROUP BY date, task_id
    )
    GROUP BY 1
),
final AS (
    SELECT
        p.date
        ,t.* EXCEPT(date)
        ,avg_pending_time
        ,avg_execution_time
    FROM 
        avg_pending_time p
        JOIN avg_ex_time a ON p.date = a.date
        JOIN total_tasks t ON p.date = t.date
)

SELECT * 
FROM 
    final