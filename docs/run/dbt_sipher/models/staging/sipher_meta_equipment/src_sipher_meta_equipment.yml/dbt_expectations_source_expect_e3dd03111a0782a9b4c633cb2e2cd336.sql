select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `sipher-data-testing`.`dbt_test__audit`.`dbt_expectations_source_expect_e3dd03111a0782a9b4c633cb2e2cd336`
    
      
    ) dbt_internal_test