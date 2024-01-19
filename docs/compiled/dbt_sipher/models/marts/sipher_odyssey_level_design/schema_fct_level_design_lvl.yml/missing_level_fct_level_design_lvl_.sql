

        WITH
        lvltb AS (
        SELECT
            DISTINCT g1.user_id,
            g1.session_id,
            g1.level_start_level_count,
            level_status
        FROM
           `sipher-data-testing`.`sipher_odyssey_level_design`.`fct_level_design_lvl` g1
        WHERE
            mode = 'SINGLE'
            AND UPPER(dungeon_id) NOT LIKE '%FTUE%' 
            AND UPPER(dungeon_id) NOT LIKE '%ENDLESS%' 
            
            )
        SELECT
        cur_lvl.*
        FROM
        lvltb cur_lvl
        LEFT JOIN
        lvltb pre_lvl
        ON
        cur_lvl.user_id = pre_lvl.user_id
        AND cur_lvl.session_id = pre_lvl.session_id
        AND cur_lvl.level_start_level_count = pre_lvl.level_start_level_count + 1
        AND pre_lvl.level_status = 'SUCCESS'
        WHERE
        TRUE
        AND cur_lvl.level_start_level_count > 1
        AND pre_lvl.level_status IS NULL

