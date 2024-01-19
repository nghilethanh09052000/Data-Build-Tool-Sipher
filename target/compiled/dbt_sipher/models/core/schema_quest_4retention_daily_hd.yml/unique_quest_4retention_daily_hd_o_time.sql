
    
    

with dbt_test__target as (

  select o_time as unique_field
  from `sipher-data-testing`.`tmp_dbt`.`quest_4retention_daily_hd`
  where o_time is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


