SELECT
    id AS user_id,
    LOWER(publicAddress) AS wallet_address,
    atherId AS ather_id,
    lootboxId AS lootbox_id,
    spaceshipPartIds AS spaceship_parts_id,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', createdAt) AS created_at,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', updatedAt) AS updated_at,
    dt AS snapshot_date_tzutc
FROM `sipher-data-platform`.`raw_loyalty_dashboard_gcs`.`gcs_external_raw_loyalty_log_open_lootbox`