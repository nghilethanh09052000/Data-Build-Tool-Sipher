WITH source AS (
    SELECT
        *
    FROM    
        {{ source('raw_meta_equipment', 'equipment') }}
)

,renamed_column AS (
    SELECT
        id,
        displayName AS display_name,
        gearType AS gear_type,
        rarity,
        tier
    FROM  
        source
)

SELECT * FROM renamed_column