WITH source AS (
    SELECT
        *
    FROM    
        `sipher-data-platform`.`raw_meta_equipment`.`sipher_game_reward`
)

,renamed_column AS (
    SELECT
        id,
        type,
        displayName AS display_name,
        rarity,
        tier,
        category
    FROM  
        source
)

SELECT * FROM renamed_column