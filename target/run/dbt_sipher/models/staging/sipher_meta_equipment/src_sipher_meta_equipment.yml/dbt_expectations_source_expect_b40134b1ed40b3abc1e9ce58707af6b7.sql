select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `sipher-data-testing`.`dbt_test__audit`.`dbt_expectations_source_expect_b40134b1ed40b3abc1e9ce58707af6b7`
    
      
    ) dbt_internal_test