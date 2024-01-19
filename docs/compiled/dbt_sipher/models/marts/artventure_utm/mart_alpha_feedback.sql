WITH fct_data AS (
  SELECT
    *
  FROM 
    `sipher-data-testing`.`artventure_utm`.`fct_firebase_events_artventure`
),
feedback AS (
  SELECT
    act_date
    ,country
    ,platform
    ,style
    ,creative_intensity
    ,template
    ,response
    ,recipe_id
    ,COUNT(DISTINCT user_id) AS cnt
  FROM 
    fct_data
  WHERE 
    event_name = 'feedback'
  GROUP BY 1,2,3,4,5,6,7,8
)

SELECT 
    * 
FROM 
    feedback