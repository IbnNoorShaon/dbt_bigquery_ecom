{{
    config(
        tags = ['prep']
    )
}}

with source as (

    select * from {{ ref('raw_order_items') }}

),

transformed as (

    select
        -- primary key
        id                                                  as order_item_id,

        -- foreign keys
        order_id,
        user_id,
        product_id,
        inventory_item_id,

        -- order item details
        status                                              as order_item_status,
        sale_price,

        -- timestamps
        created_at                                          as order_item_created_at,
        shipped_at                                          as order_item_shipped_at,
        delivered_at                                        as order_item_delivered_at,
        returned_at                                         as order_item_returned_at,

        -- business logic
        case
            when status = 'Complete'    then true
            else                             false
        end                                                 as is_completed,

        case
            when status = 'Returned'    then true
            else                             false
        end                                                 as is_returned,

        case
            when status = 'Cancelled'   then true
            else                             false
        end                                                 as is_cancelled,

        round(sale_price, 2)                                as sale_price_rounded,

        -- audit columns
        {{ audit_columns() }}

    from source

)

select * from transformed
