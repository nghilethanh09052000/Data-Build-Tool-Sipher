with relation_columns as (

        
        select
            cast('IDENTITY_LINEITEMID' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('BILL_BILLINGPERIODSTARTDATE' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('LINEITEM_USAGESTARTDATE' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('LINEITEM_PRODUCTCODE' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('LINEITEM_UNBLENDEDCOST' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('LINEITEM_BLENDEDCOST' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('LINEITEM_LINEITEMDESCRIPTION' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('LINEITEM_LINEITEMTYPE' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('PRODUCT_REGION' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('RESOURCETAGS_USER_EKS_CLUSTER_NAME' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('RESOURCETAGS_USER_PROJECT' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('RESOURCETAGS_USER_TYPE' as STRING) as relation_column,
            cast('STRING' as STRING) as relation_column_type
        union all
        
        select
            cast('DT' as STRING) as relation_column,
            cast('DATE' as STRING) as relation_column_type
        
        
    ),
    test_data as (

        select
            *
        from
            relation_columns
        where
            relation_column = 'DT'
            and
            relation_column_type not in ('DATE')

    )
    select *
    from test_data