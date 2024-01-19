with relation_columns as (

        
        select
            cast('EVENT_TIME' as STRING) as relation_column,
            cast('DATE' as STRING) as relation_column_type
        union all
        
        select
            cast('USER_ID' as STRING) as relation_column,
            cast('INT64' as STRING) as relation_column_type
        union all
        
        select
            cast('WALLET_ADDRESS' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('USER_PSEUDO_ID' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('TRAFFIC_SOURCE' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('COUNTRY' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('QUEST_USERID' as STRING) as relation_column,
            cast('INT64' as STRING) as relation_column_type
        union all
        
        select
            cast('QUESTTITLE' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('TRANS_QUEST_ID' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('LIFETIME' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('POINT_USERID' as STRING) as relation_column,
            cast('INT64' as STRING) as relation_column_type
        union all
        
        select
            cast('TOTALXP' as STRING) as relation_column,
            cast('FLOAT64' as STRING) as relation_column_type
        union all
        
        select
            cast('TOTALNANOCHIPS' as STRING) as relation_column,
            cast('FLOAT64' as STRING) as relation_column_type
        union all
        
        select
            cast('RANK_' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('LEVEL' as STRING) as relation_column,
            cast('INT64' as STRING) as relation_column_type
        union all
        
        select
            cast('SIPHERNFTS' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('SIPHERTOKENS' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        
        
    ),
    test_data as (

        select
            *
        from
            relation_columns
        where
            relation_column = 'QUEST_USERID'
            and
            relation_column_type not in ('STRING')

    )
    select *
    from test_data