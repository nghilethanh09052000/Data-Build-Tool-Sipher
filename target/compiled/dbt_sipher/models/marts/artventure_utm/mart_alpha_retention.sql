WITH fct_data AS (
  SELECT
    *
  FROM 
    `sipher-data-testing`.`artventure_utm`.`fct_firebase_events_artventure`
)
,user_cnt_day0 AS (
  SELECT
    cohort_date
    ,COUNT(DISTINCT user_id) AS cnt_users_day0
  FROM 
    fct_data
  WHERE 
    day_diff = 0
  GROUP BY 1
)
,user_cnt_day0_by_recipe AS (
  SELECT
    cohort_date
    ,recipe_id
    ,COUNT(DISTINCT user_id) AS cnt_users_day0_by_recipe
  FROM 
    fct_data
  WHERE 
    day_diff = 0
  GROUP BY 1,2
)

SELECT
  *
FROM 
  fct_data f 
  LEFT JOIN user_cnt_day0 u         USING(cohort_date)
  LEFT JOIN user_cnt_day0_by_recipe USING(cohort_date, recipe_id)