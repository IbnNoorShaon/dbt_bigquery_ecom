{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'order_item_id',
        schema = 'serve',
        tags = ['serve', 'fct'],
        partition_by = {
            "field": "partition_key",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by = ['order_item_created_at', 'order_item_status', 'product_id']
    )
}}

with order_items as (

    select * from {{ ref('prep_order_items') }}

    {% if is_incremental() %}
        where order_item_created_at > (select max(order_item_created_at) from {{ this }})
    {% endif %}

),

products as (

    select
        product_id,
        product_name,
        product_category,
        product_brand,
        product_department,
        product_price_tier,
        product_cost,
        product_retail_price,
        product_gross_margin,
        product_margin_pct
    from {{ ref('prep_products') }}

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
        order_items.order_item_id,

        -- foreign keys
        order_items.order_id,
        order_items.user_id                                 as customer_id,
        order_items.product_id,
        order_items.inventory_item_id,

        -- order item details
        order_items.order_item_status,
        order_items.sale_price,
        order_items.sale_price_rounded,

        -- timestamps
        order_items.order_item_created_at,
        order_items.order_item_shipped_at,
        order_items.order_item_delivered_at,
        order_items.order_item_returned_at,

        -- status flags
        order_items.is_completed,
        order_items.is_returned,
        order_items.is_cancelled,

        -- product details (pre-joined for Looker)
        products.product_name,
        products.product_category,
        products.product_brand,
        products.product_department,
        products.product_price_tier,
        products.product_cost,
        products.product_retail_price,
        products.product_gross_margin,
        products.product_margin_pct,

        -- derived metrics
        round(order_items.sale_price - products.product_cost, 2)    as item_profit,
        round(safe_divide(
            order_items.sale_price - products.product_cost,
            order_items.sale_price
        ) * 100, 2)                                                  as item_margin_pct,

        -- batch run keys
        batch_run.partition_key,
        batch_run.run_datetime

    from order_items
    left join products
        on order_items.product_id = products.product_id
    cross join batch_run

)

select * from transformed
