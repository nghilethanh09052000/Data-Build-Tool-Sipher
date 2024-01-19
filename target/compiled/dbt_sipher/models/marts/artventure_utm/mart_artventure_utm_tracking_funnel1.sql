WITH fct AS (
  SELECT
    *
  FROM 
    `sipher-data-testing`.`artventure_utm`.`fct_artventure_utm_tracking_events`
)
,first_visit AS (
  SELECT
    event_date
    ,ts
    ,user_id
    ,user_pseudo_id
    ,recipe
    ,source
    ,medium
    ,campaign
    ,session_cnt
    ,session_time_sec
    ,'first visit' AS step
  FROM 
    fct
  WHERE 
    event_name = 'first_visit'
)
,generate AS (
  SELECT 
    DISTINCT * 
  FROM (
    SELECT
      f.event_date
      ,f.ts
      ,f.user_id
      ,f.user_pseudo_id
      ,f.recipe
      ,f.source
      ,f.medium
      ,f.campaign
      ,f.session_cnt
      ,f.session_time_sec
      ,'generate' AS step
    FROM 
      first_visit v
      LEFT JOIN fct f 
      USING(user_pseudo_id)
    WHERE 
        event_name = 'click'
        AND (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = "event_label") = 'recipe_alpha_generate'
  )
)
,signup AS (
  SELECT
    g.event_date
    ,g.ts
    ,g.user_id
    ,g.user_pseudo_id
    ,g.recipe
    ,g.source
    ,g.medium
    ,g.campaign
    ,g.session_cnt
    ,g.session_time_sec
    ,'signup' AS signup
  FROM 
    generate g
    LEFT JOIN fct f
    ON g.event_date < f.event_date
    AND g.user_pseudo_id = f.user_pseudo_id
  WHERE 
    event_name = 'login'
)
,combine AS (
  SELECT * FROM first_visit
  
  UNION ALL
  
  SELECT * FROM generate
  
  UNION ALL
  
  SELECT * FROM signup
)
,final AS (
  SELECT
    *,
    CASE WHEN step = 'generate' THEN ROW_NUMBER() OVER(PARTITION BY step, user_id, user_pseudo_id ORDER BY ts) ELSE NULL END AS generate_times
  FROM 
    combine
)

SELECT *,
    MAX(generate_times) OVER(PARTITION BY step,user_id, user_pseudo_id) AS max_generate_times
FROM 
    final