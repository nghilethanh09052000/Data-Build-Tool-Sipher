SELECT
  video_id,
  comment_text,
  comment_user_id,
  snapshot_date_tzict
FROM `sipher-data-testing`.`staging_social`.`stg_tiktok_video_comment`
WHERE comment_text IS NOT NULL