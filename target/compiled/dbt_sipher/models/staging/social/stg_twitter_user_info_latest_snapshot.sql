SELECT
  id,
  username,
  name,
  created_at AS user_created_at,
  description,
  verified AS is_verified,
  location AS user_defined_location,
  profile_image_url,
  url,
  entities.description.cashtags,
  entities.description.hashtags,
  entities.description.mentions,
  entities.description.urls AS entities_urls,
  entities.url.urls,
  protected AS is_protected,
  pinned_tweet_id,

  public_metrics.followers_count AS followers_cnt,
  public_metrics.following_count AS following_cnt,
  public_metrics.listed_count AS listed_cnt,
  public_metrics.tweet_count AS tweet_cnt
FROM `sipher-data-platform`.`raw_social`.`twitter_user_info__SIPHERxyz__*`
WHERE _TABLE_SUFFIX = "20230411"