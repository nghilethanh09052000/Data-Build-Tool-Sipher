select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `sipher-data-testing`.`dbt_test__audit`.`source_not_null_raw_meta_equipment_weapon_id`
    
      
    ) dbt_internal_test