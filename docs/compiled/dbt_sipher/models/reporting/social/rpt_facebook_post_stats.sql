WITH
fct AS (
  SELECT *
  FROM `sipher-data-testing`.`mart_social`.`fct_facebook_post_lifetime`
)

,dim AS (
  SELECT
    id,
    text,
    post_url,
    post_created_at
  FROM `sipher-data-testing`.`mart_social`.`dim_facebook_post`
)

SELECT
  post_created_at,
  DATE(post_created_at) AS date,
  post_id,
  text,
  post_url,
  impressions,
  reach,
  engagement,
  reactions,
  likes,
  loves,
  wows,
  hahas,
  sorries,
  angers,
  clicks,
  comments,
  shares
FROM fct
LEFT JOIN dim ON fct.post_id = dim.id