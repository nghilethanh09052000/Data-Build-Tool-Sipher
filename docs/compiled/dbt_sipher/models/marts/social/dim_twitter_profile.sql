SELECT
  id,
  MAX(username) AS username,
  MAX(name) AS name,
  MAX(description) AS description,
  MAX(is_verified) AS is_verified,
  MAX(is_protected) AS is_protected,
  MAX(user_defined_location) AS user_defined_location,
  MAX(user_defined_url) AS user_defined_url,
  MAX(pinned_tweet_id) AS pinned_tweet_id,
  MAX(profile_created_at) AS profile_created_at
FROM `sipher-data-testing`.`staging_social`.`stg_twitter_profile_stats`
WHERE snapshot_date = "2024-01-18"
GROUP BY id