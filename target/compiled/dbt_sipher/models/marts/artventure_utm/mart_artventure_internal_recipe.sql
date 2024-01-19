WITH fact AS (
  SELECT
    * EXCEPT(act_date, ts)
  FROM 
    `sipher-data-testing`.`artventure_utm`.`fct_artventure_internal_events`
)
,mart AS (
  SELECT
    * EXCEPT(user_id)
    ,COUNT(DISTINCT user_id) AS cnt
  FROM 
    fact
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)

SELECT 
  * 
FROM
  mart