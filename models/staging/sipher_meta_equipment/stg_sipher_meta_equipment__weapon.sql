WITH source AS (
    SELECT
        *
    FROM    
        {{ source('raw_meta_equipment', 'weapon') }}
)

,renamed_column AS (
    SELECT
        id,
        displayName AS display_name,
        weaponType AS weapon_type,
        rarity,
        tier,
        assetId AS asset_id
    FROM  
        source
)

SELECT * FROM renamed_column ORDER BY id