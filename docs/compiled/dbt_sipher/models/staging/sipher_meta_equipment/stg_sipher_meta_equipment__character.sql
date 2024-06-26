WITH source AS (
    SELECT
        *
    FROM    
        `sipher-data-platform`.`raw_meta_equipment`.`character`
)

,renamed_column AS (
    SELECT
        id,
        name,
        subRace AS sub_race,
        variants,
        accessories,
        body,
        hands,
        legs,
        feet
    FROM  
        source
)

SELECT * FROM renamed_column