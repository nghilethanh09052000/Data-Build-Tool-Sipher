select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `sipher-data-testing`.`dbt_test__audit`.`dbt_expectations_source_expect_c7b6ee721ecc5a29a480066d7855b1e9`
    
      
    ) dbt_internal_test