SELECT
  id,
  unique_id AS user_name,
  nickname,
  region AS country_code
FROM `sipher-data-platform`.`raw_social`.`tiktok_user_info`