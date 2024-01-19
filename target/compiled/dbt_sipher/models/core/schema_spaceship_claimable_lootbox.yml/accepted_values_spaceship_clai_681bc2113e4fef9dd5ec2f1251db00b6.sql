
    
    

with all_values as (

    select
        tokenId as value_field,
        count(*) as n_records

    from `sipher-data-testing`.`tmp_dbt`.`spaceship_claimable_lootbox`
    group by tokenId

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6','-1'
)


