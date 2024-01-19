select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `sipher-data-testing`.`dbt_test__audit`.`dbt_expectations_source_expect_687dab4e3e8f689810184d28ab8c6234`
    
      
    ) dbt_internal_test