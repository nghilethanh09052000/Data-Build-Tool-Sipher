select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `sipher-data-testing`.`dbt_test__audit`.`dbt_expectations_source_expect_94a16fa37e28505fa0bf7adcc430b534`
    
      
    ) dbt_internal_test