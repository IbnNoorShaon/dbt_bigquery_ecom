{{
    config(
        tags = ['raw']
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

    select * from {{ source('thelook_ecommerce', 'products') }}

),

raw as (

    select
        {{ generate_cast(columns) }},
        {{ audit_columns() }}

    from source

)

select * from raw
