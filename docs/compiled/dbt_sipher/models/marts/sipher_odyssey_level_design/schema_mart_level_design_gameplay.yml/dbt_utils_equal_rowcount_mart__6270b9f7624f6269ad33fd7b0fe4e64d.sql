




with a as (

    select 
      
      1 as id_dbtutils_test_equal_rowcount,
      count(*) as count_a 
    from `sipher-data-testing`.`sipher_odyssey_level_design`.`mart_level_design_gameplay`
    group by id_dbtutils_test_equal_rowcount


),
b as (

    select 
      
      1 as id_dbtutils_test_equal_rowcount,
      count(*) as count_b 
    from `sipher-data-testing`.`sipher_odyssey_level_design`.`fct_level_design_gameplay`
    group by id_dbtutils_test_equal_rowcount

),
final as (

    select
    
        a.id_dbtutils_test_equal_rowcount as id_dbtutils_test_equal_rowcount_a,
          b.id_dbtutils_test_equal_rowcount as id_dbtutils_test_equal_rowcount_b,
        

        count_a,
        count_b,
        abs(count_a - count_b) as diff_count

    from a
    full join b
    on
    a.id_dbtutils_test_equal_rowcount = b.id_dbtutils_test_equal_rowcount
    


)

select * from final

