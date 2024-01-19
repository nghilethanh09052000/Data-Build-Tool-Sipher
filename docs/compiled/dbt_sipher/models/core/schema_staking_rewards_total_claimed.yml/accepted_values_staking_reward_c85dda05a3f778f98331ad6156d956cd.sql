
    
    

with all_values as (

    select
        platform as value_field,
        count(*) as n_records

    from `sipher-data-testing`.`tmp_dbt`.`staking_rewards_total_claimed`
    group by platform

)

select *
from all_values
where value_field not in (
    'lp_uniswap','single_side','lp_kyberswap'
)


