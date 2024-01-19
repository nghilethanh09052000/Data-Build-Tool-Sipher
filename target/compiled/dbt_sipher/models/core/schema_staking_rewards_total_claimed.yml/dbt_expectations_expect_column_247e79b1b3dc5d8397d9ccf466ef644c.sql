





    with grouped_expression as (
    select
        
        
    
  
( 1=1 and length(
        wallet_address
    ) > 41 and length(
        wallet_address
    ) < 43
)
 as expression


    from `sipher-data-testing`.`tmp_dbt`.`staking_rewards_total_claimed`
    where
        wallet_address is not null
    
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors






