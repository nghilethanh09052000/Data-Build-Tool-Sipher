select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `sipher-data-testing`.`dbt_test__audit`.`dbt_expectations_source_expect_85ca607f9d7df93d84d8a8dca1f672ac`
    
      
    ) dbt_internal_test