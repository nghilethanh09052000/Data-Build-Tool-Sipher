
with exceptions as (
      SELECT
              *        
      FROM `sipher-data-testing`.`sipher_odyssey_level_design`.`fct_level_design_gameplay`
      WHERE TRUE 
        AND IFNULL(gameplay_down,0) > 0 AND IFNULL(gameplay_hp_loss,0) = 0
      )

select * from exceptions

