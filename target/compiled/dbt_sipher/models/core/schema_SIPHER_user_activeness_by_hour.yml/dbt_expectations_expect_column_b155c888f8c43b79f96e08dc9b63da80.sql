with relation_columns as (

        
    ),
    test_data as (

        select
            *
        from
            relation_columns
        where
            relation_column = 'HOUR'
            and
            relation_column_type not in ('INT64')

    )
    select *
    from test_data