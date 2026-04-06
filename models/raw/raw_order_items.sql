{{
    config(
        tags = ['raw']
    )
}}

{% set columns = {
    "id"                : "INT64",
    "order_id"          : "INT64",
    "user_id"           : "INT64",
    "product_id"        : "INT64",
    "inventory_item_id" : "INT64",
    "status"            : "STRING",
    "sale_price"        : "FLOAT64",
    "created_at"        : "TIMESTAMP",
    "shipped_at"        : "TIMESTAMP",
    "delivered_at"      : "TIMESTAMP",
    "returned_at"       : "TIMESTAMP"
} %}

with source as (

    select * from {{ source('thelook_ecommerce', 'order_items') }}

),

raw as (

    select
        {{ generate_cast(columns) }},
        {{ audit_columns() }}

    from source

)

select * from raw
