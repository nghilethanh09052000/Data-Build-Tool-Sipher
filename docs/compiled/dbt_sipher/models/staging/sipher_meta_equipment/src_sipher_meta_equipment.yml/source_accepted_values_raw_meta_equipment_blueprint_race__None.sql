
    
    

with all_values as (

    select
        race as value_field,
        count(*) as n_records

    from (select * from `sipher-data-platform`.`raw_meta_equipment`.`blueprint` where gearType IS NULL) dbt_subquery
    group by race

)

select *
from all_values
where value_field not in (
    'None'
)


