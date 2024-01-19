





    with grouped_expression as (
    select
        
        
    
  
( 1=1 and length(
        wallet_address
    ) > 42 and length(
        wallet_address
    ) < 44
)
 as expression


    from `sipher-data-testing`.`tmp_dbt`.`quest_dashboard_hd`
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






