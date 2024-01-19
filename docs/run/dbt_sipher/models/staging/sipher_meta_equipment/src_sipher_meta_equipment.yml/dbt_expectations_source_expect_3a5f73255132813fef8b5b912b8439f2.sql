select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `sipher-data-testing`.`dbt_test__audit`.`dbt_expectations_source_expect_3a5f73255132813fef8b5b912b8439f2`
    
      
    ) dbt_internal_test