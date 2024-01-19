
    
    

with all_values as (

    select
        lootbox_type as value_field,
        count(*) as n_records

    from `sipher-data-testing`.`tmp_dbt`.`spaceship_claimable_lootbox`
    group by lootbox_type

)

select *
from all_values
where value_field not in (
    'Akagi','Alice','Flik Flak','Ahab','Zed','Baron','Tunku','Mystery'
)


