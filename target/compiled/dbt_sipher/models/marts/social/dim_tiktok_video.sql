SELECT
  video_id AS id,
  MAX(video_description) AS description,
  MAX(video_music) AS music
FROM `sipher-data-testing`.`staging_social`.`stg_tiktok_video_comment`
WHERE snapshot_date_tzict = '2022-12-21'
GROUP BY id