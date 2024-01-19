select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `sipher-data-testing`.`dbt_test__audit`.`dbt_expectations_source_expect_64df7e5ebe46ff479ae0c779d0dbb160`
    
      
    ) dbt_internal_test