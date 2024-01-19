SELECT
    id AS user_id,
    tokenId AS token_id,
    LOWER(publicAddress) AS wallet_address,
    atherId AS ather_id,
    name,
    partTokenIds AS part_token_ids,
    action,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', createdAt) AS created_at,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', updatedAt) AS updated_at,
    dt AS snapshot_date_tzutc
FROM `sipher-data-platform`.`raw_loyalty_dashboard_gcs`.`gcs_external_raw_loyalty_log_spaceship`