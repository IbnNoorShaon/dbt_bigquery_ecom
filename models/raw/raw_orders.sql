{{
    config(
        tags = ['raw']
    )
}}

{% set columns = {
    "order_id"      : "INT64",
    "user_id"       : "INT64",
    "status"        : "STRING",
    "gender"        : "STRING",
    "num_of_item"   : "INT64",
    "created_at"    : "TIMESTAMP",
    "shipped_at"    : "TIMESTAMP",
    "delivered_at"  : "TIMESTAMP",
    "returned_at"   : "TIMESTAMP"
} %}

with source as (

    select * from {{ source('thelook_ecommerce', 'orders') }}

),

raw as (

    select
        {{ generate_cast(columns) }},
        {{ audit_columns() }}

    from source

)

select * from raw
