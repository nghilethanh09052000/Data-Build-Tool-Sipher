WITH source AS (
    SELECT
        *
    FROM    
        {{ source('raw_meta_equipment', 'blueprint') }}
)

,renamed_column AS (
    SELECT
        id,
        displayName AS display_name,
        type,
        gearType AS gear_type,
        CASE WHEN race = 'Buru' THEN 'BURU' ELSE race END AS race,
        weaponType AS weapon_type,
        baseId AS base_id,
        rarity,
        tier
    FROM  
        source
)

SELECT DISTINCT race FROM renamed_column
