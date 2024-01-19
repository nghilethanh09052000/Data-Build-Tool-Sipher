select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `sipher-data-testing`.`dbt_test__audit`.`source_accepted_values_raw_met_b921c5284f07d6237a1e6ad4800cc454`
    
      
    ) dbt_internal_test