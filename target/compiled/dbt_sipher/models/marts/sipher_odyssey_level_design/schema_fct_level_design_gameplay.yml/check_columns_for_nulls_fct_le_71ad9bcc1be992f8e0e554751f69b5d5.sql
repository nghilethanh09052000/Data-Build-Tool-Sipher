
with exceptions as (
      SELECT
      COUNT(*) AS error_count
        
      FROM `sipher-data-testing`.`sipher_odyssey_level_design`.`fct_level_design_gameplay`
      WHERE TRUE 
      AND 1=1
      AND

      gameplay_start_event_date IS NULL  
      AND

      gameplay_start_event_timestamp IS NULL  
      AND

      build_number IS NULL  
      AND

      app_version IS NULL  
      AND

      email IS NULL  
      AND

      user_name IS NULL  
      AND

      user_pseudo_id IS NULL  
      AND

      user_id IS NULL  
      AND

      day0_date_tzutc IS NULL  
      AND

      session_id IS NULL  
      AND

      dungeon_id IS NULL  
      AND

      mode IS NULL  
      AND

      difficulty IS NULL  
      AND

      sub_race IS NULL  
      AND

      character_level IS NULL  
      AND

      character_PS IS NULL  
      AND

      armor IS NULL  
      AND

      armor_PS IS NULL  
      AND

      head IS NULL  
      AND

      head_PS IS NULL  
      AND

      shoes IS NULL  
      AND

      shoes_PS IS NULL  
      AND

      legs IS NULL  
      AND

      legs_PS IS NULL  
      AND

      gloves IS NULL  
      AND

      gloves_PS IS NULL  
      AND

      weapon1 IS NULL  
      AND

      weapon1_PS IS NULL  
      AND

      weapon2 IS NULL  
      AND

      weapon2_PS IS NULL  
      
      )

select * from exceptions
where error_count > 0

