SELECT
    user_id,
    Username AS username,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', UserCreateDate) AS user_create_date,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', UserLastModifiedDate) AS user_last_modified_date,
    Attributes AS attributes,
    connected_wallets,
    email,
    email_verified,
    identities,
    CAST(
        (
            CASE WHEN Enabled = 'nan' THEN NULL ELSE Enabled END
        ) AS BOOLEAN
    ) AS is_enabled,
    name,
    sub,
    UserStatus AS user_status,
    dt AS snapshot_date_tzutc
FROM `sipher-data-platform`.`raw_atherid_gcs`.`gcs_external_raw_ather_id_user_cognito`