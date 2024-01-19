WITH source AS (
    SELECT
        *
    FROM    
        `sipher-data-platform`.`raw_meta_equipment`.`equipment_part`
)

,renamed_column AS (
    SELECT
        id,
        displayName,
        rarity,
        tier,
        boostPools_List AS boost_pools_list,  
        assetId AS asset_id,  
        partType AS part_type,  
        partModelId AS part_model_id,
        weaponType AS weapon_type,
    FROM  
        source
    WHERE 
        id IS NOT NULL
)

SELECT * FROM renamed_column