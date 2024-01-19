SELECT DISTINCT
  id,
  name,
  profile_created_at,
  description,
  owner,
  owner_id
FROM `sipher-data-testing`.`staging_social`.`stg_discord_profile_stats_snapshot`
WHERE snapshot_date_tzict = '2023-05-30'