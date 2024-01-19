select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `sipher-data-testing`.`dbt_test__audit`.`source_accepted_values_raw_meta_equipment_blueprint_race__None`
    
      
    ) dbt_internal_test