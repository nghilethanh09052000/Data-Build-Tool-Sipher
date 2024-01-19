WITH source AS (
    SELECT
        *
    FROM    
        {{ source('raw_meta_equipment', 'capsule') }}
)

,renamed_column AS (
    SELECT
        *
    FROM  
        source
)

SELECT * FROM renamed_column