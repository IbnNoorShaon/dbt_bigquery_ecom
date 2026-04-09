{{
    config(
        tags = ['prep']
    )
}}

{% set columns = {
    "id"                    : "INT64",
    "name"                  : "STRING",
    "category"              : "STRING",
    "brand"                 : "STRING",
    "department"            : "STRING",
    "sku"                   : "STRING",
    "cost"                  : "FLOAT64",
    "retail_price"          : "FLOAT64",
    "distribution_center_id": "INT64"
} %}

with source as (

    select * from {{ ref('raw_products') }}

),

transform as (

    select
        -- primary key
        id                                                      as product_id,

        -- product details
        name                                                    as product_name,
        category                                                as product_category,
        brand                                                   as product_brand,
        department                                              as product_department,
        sku                                                     as product_sku,

        -- distribution
        distribution_center_id,

        -- pricing
        cost                                                    as product_cost,
        retail_price                                            as product_retail_price,

        -- business logic
        round(retail_price - cost, 2)                           as product_gross_margin,
        round(safe_divide(retail_price - cost, retail_price)
              * 100, 2)                                         as product_margin_pct,

        case
            when retail_price < 20      then 'Budget'
            when retail_price < 50      then 'Mid-Range'
            when retail_price < 100     then 'Premium'
            else                             'Luxury'
        end                                                     as product_price_tier,

        -- audit columns
        {{ audit_columns() }}

    from source

)

select * from transform
