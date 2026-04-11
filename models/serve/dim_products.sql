{{
    config(
        materialized = 'table',
        schema = 'serve',
        tags = ['serve', 'dim']
    )
}}

with products as (

    select * from {{ ref('prep_products') }}

),


transformed as (

    select
        -- primary key
        product_id,

        -- product details
        product_name,
        product_category,
        product_brand,
        product_department,
        product_sku,

        -- distribution
        distribution_center_id,

        -- pricing
        product_cost,
        product_retail_price,
        product_gross_margin,
        product_margin_pct,
        product_price_tier,

    from products

)

select * from transformed
