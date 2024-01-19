SELECT
  c.item.id AS comment_id,
  post_id
FROM `sipher-data-testing`.`staging_social`.`stg_facebook_sipher_post_comments`, UNNEST(comments) AS c