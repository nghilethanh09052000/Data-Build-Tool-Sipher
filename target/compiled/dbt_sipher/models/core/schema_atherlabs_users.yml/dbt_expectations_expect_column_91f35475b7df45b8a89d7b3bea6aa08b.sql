





    with grouped_expression as (
    select
        
        
    
  
( 1=1 and length(
        wallet_address
    ) >= 42 and length(
        wallet_address
    ) <= 42
)
 as expression


    from `sipher-data-testing`.`tmp_dbt`.`atherlabs_users`
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






