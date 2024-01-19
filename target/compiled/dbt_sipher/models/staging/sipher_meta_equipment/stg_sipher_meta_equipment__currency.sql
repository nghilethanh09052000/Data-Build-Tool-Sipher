WITH source AS (
    SELECT
        *
    FROM    
        `sipher-data-platform`.`raw_meta_equipment`.`currency`
)

,renamed_column AS (
    SELECT
        id,
        categories,
        baseId AS base_id,
        rarity,
        displayName AS display_name
    FROM  
        source
)

SELECT * FROM renamed_column