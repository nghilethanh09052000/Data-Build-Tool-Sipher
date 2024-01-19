WITH raw_feedback AS (
    SELECT
        CASE WHEN user_id = 'anonymous' THEN user_pseudo_id ELSE user_id END AS user_id,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "recipe_id") AS recipe,
        (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "response") AS response
    FROM `sipher-data-testing`.`tmp_dbt`.`fct_artventure_user_events`
    WHERE event_name = 'click'
        AND (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "event_label") = "feedback"
)
,reporting AS (
    SELECT
        recipe,SUM(CASE WHEN response = 'CLOSE' THEN 1 ELSE 0 END) AS cnt_CLOSE,
        SUM(CASE WHEN response = 'HAPPY' THEN 1 ELSE 0 END) AS cnt_HAPPY,
        SUM(CASE WHEN response = 'NEUTRAL' THEN 1 ELSE 0 END) AS cnt_NEUTRAL,
        SUM(CASE WHEN response = 'SAD' THEN 1 ELSE 0 END) AS cnt_SAD,
        SUM(CASE WHEN response = 'SKIP' THEN 1 ELSE 0 END) AS cnt_SKIP,
        FROM raw_feedback
    WHERE recipe IS NOT NULL
    GROUP BY recipe
)

SELECT * FROM reporting