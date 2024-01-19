select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `sipher-data-testing`.`dbt_test__audit`.`dbt_expectations_source_expect_933b47e689d09630973c696d5d694492`
    
      
    ) dbt_internal_test