

  create or replace view `sipher-data-testing`.`tmp_dbt`.`stg_sipher_meta_equipment__equipment_part`
  OPTIONS()
  as WITH source AS (
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
        primaryStats_List AS primary_stats_list,
        boostPools_List AS boost_pools_list,  
        secondaryStatPoolId AS secondary_stat_pool_id, 
        assetId AS asset_id,  
        partType AS part_type,  
        partModelId AS part_model_id,
        weaponType AS weapon_type,
    FROM  
        source
)

SELECT * FROM renamed_column;

