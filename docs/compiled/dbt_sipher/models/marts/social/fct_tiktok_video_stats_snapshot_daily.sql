SELECT
  snapshot_date_tzict,
  video_id,
  MAX(comments_cnt) AS comments_cnt,
  MAX(diggs_cnt) AS diggs_cnt,
  MAX(download_cnt) AS download_cnt,
  MAX(play_cnt) AS play_cnt,
  MAX(forward_cnt) AS forward_cnt,
  MAX(share_cnt) AS share_cnt,
FROM `sipher-data-testing`.`staging_social`.`stg_tiktok_video_comment`
GROUP BY video_id, snapshot_date_tzict