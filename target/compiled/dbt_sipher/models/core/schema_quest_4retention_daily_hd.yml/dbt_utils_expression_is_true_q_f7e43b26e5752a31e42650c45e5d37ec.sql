



select
    *
from `sipher-data-testing`.`tmp_dbt`.`quest_4retention_daily_hd`

where not(returns  >= 0)

