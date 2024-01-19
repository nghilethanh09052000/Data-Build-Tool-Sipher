WITH
post_insights AS (
  SELECT
    post_id,COALESCE(MAX(CASE WHEN metric_name = 'post_impressions' THEN value END), 0) AS post_impressions,
    COALESCE(MAX(CASE WHEN metric_name = 'post_impressions_unique' THEN value END), 0) AS post_impressions_unique,
    COALESCE(MAX(CASE WHEN metric_name = 'post_reactions_like_total' THEN value END), 0) AS post_reactions_like_total,
    COALESCE(MAX(CASE WHEN metric_name = 'post_reactions_love_total' THEN value END), 0) AS post_reactions_love_total,
    COALESCE(MAX(CASE WHEN metric_name = 'post_reactions_wow_total' THEN value END), 0) AS post_reactions_wow_total,
    COALESCE(MAX(CASE WHEN metric_name = 'post_reactions_haha_total' THEN value END), 0) AS post_reactions_haha_total,
    COALESCE(MAX(CASE WHEN metric_name = 'post_reactions_sorry_total' THEN value END), 0) AS post_reactions_sorry_total,
    COALESCE(MAX(CASE WHEN metric_name = 'post_reactions_anger_total' THEN value END), 0) AS post_reactions_anger_total,
    COALESCE(MAX(CASE WHEN metric_name = 'post_clicks' THEN value END), 0) AS post_clicks,
    FROM `sipher-data-testing`.`staging_social`.`stg_facebook_sipher_post_insights`
  GROUP BY post_id
)

,post_shares AS (
  SELECT
    id AS post_id,
    SUM(COALESCE(shares, 0)) AS shares
    FROM `sipher-data-testing`.`staging_social`.`stg_facebook_sipher_page_feed`
  GROUP BY post_id
)

,post_comments AS (
  SELECT
    post_id,
    COUNT(*) AS comments
    FROM `sipher-data-testing`.`mart_social`.`dim_facebook_post_comment`
  GROUP BY post_id
)

SELECT
  post_id,
  COALESCE(post_impressions, 0) AS impressions,
  COALESCE(post_impressions_unique, 0) AS reach,
  COALESCE(post_reactions_like_total, 0)
    + COALESCE(post_reactions_love_total, 0)
    + COALESCE(post_reactions_wow_total, 0)
    + COALESCE(post_reactions_haha_total, 0)
    + COALESCE(post_reactions_sorry_total, 0)
    + COALESCE(post_reactions_anger_total, 0)
    + COALESCE(comments, 0)
    + COALESCE(post_clicks, 0)
    + COALESCE(shares, 0)
  AS engagement,
  COALESCE(post_reactions_like_total, 0)
    + COALESCE(post_reactions_love_total, 0)
    + COALESCE(post_reactions_wow_total, 0)
    + COALESCE(post_reactions_haha_total, 0)
    + COALESCE(post_reactions_sorry_total, 0)
    + COALESCE(post_reactions_anger_total, 0)
  AS reactions,
  COALESCE(post_reactions_like_total, 0) AS likes,
  COALESCE(post_reactions_love_total, 0) AS loves,
  COALESCE(post_reactions_wow_total, 0) AS wows,
  COALESCE(post_reactions_haha_total, 0) AS hahas,
  COALESCE(post_reactions_sorry_total, 0) AS sorries,
  COALESCE(post_reactions_anger_total, 0) AS angers,
  COALESCE(post_clicks, 0) AS clicks,
  COALESCE(comments, 0) AS comments,
  COALESCE(shares, 0) AS shares
FROM post_insights
FULL OUTER JOIN post_shares USING(post_id)
FULL OUTER JOIN post_comments USING(post_id)