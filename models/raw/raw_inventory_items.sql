{{
    config(
        tags = ['raw']
    )
}}

{% set columns = {
    "id"                            : "INT64",
    "product_id"                    : "INT64",
    "created_at"                    : "TIMESTAMP",
    "sold_at"                       : "TIMESTAMP",
    "cost"                          : "FLOAT64",
    "product_category"              : "STRING",
    "product_name"                  : "STRING",
    "product_brand"                 : "STRING",
    "product_retail_price"          : "FLOAT64",
    "product_department"            : "STRING",
    "product_sku"                   : "STRING",
    "product_distribution_center_id": "INT64"
} %}

with source as (

    select * from {{ source('thelook_ecommerce', 'inventory_items') }}

),

raw as (

    select
        {{ generate_cast(columns) }},
        {{ audit_columns() }}

    from source

)

select * from raw
