{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'order_id',
        schema = 'serve',
        tags = ['serve', 'fct'],
        partition_by = {
            "field": "partition_key",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by = ['order_created_date', 'order_status', 'customer_id']
    )
}}

with orders as (

    select * from {{ ref('prep_orders') }}

    {% if is_incremental() %}
        where order_created_at > (select max(order_created_at) from {{ this }})
    {% endif %}

),

order_items as (

    select
        order_id,
        sum(sale_price)                                     as order_revenue,
        count(*)                                            as order_item_count,
        sum(case when is_returned then 1 else 0 end)        as returned_items,
        sum(case when is_cancelled then 1 else 0 end)       as cancelled_items
    from {{ ref('prep_order_items') }}
    group by 1

),

batch_run as (

    select
        partition_key,
        run_datetime
    from {{ ref('batch_run') }}

),

transformed as (

    select
        -- primary key
        orders.order_id,

        -- foreign keys
        orders.user_id                                      as customer_id,

        -- order details
        orders.order_status,
        orders.order_gender,
        orders.order_num_items,

        -- timestamps
        orders.order_created_at,
        orders.order_created_date,
        orders.order_year,
        orders.order_month,
        orders.order_day_of_week,
        orders.order_shipped_at,
        orders.order_delivered_at,
        orders.order_returned_at,

        -- revenue metrics
        order_items.order_revenue,
        order_items.order_item_count,
        order_items.returned_items,
        order_items.cancelled_items,

        -- derived metrics
        round(safe_divide(
            order_items.order_revenue,
            order_items.order_item_count
        ), 2)                                               as avg_item_price,

        orders.days_to_deliver,

        -- status flags
        orders.is_completed,
        orders.is_returned,
        orders.is_cancelled,

        -- batch run keys
        batch_run.partition_key,
        batch_run.run_datetime

    from orders
    left join order_items
        on orders.order_id = order_items.order_id
    cross join batch_run

)

select * from transformed
