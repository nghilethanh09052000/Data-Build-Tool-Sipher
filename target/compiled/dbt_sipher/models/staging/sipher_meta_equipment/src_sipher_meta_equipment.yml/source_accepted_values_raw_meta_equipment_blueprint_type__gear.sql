
    
    

with all_values as (

    select
        type as value_field,
        count(*) as n_records

    from (select * from `sipher-data-platform`.`raw_meta_equipment`.`blueprint` where gearType IS NOT NULL) dbt_subquery
    group by type

)

select *
from all_values
where value_field not in (
    'Gear'
)


