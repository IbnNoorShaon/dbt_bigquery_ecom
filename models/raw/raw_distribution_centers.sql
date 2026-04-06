{{
    config(
        tags = ['raw']
    )
}}

{% set columns = {
    "id"        : "INT64",
    "name"      : "STRING",
    "latitude"  : "FLOAT64",
    "longitude" : "FLOAT64"
} %}

with source as (

    select * from {{ source('thelook_ecommerce', 'distribution_centers') }}

),

raw as (

    select
        {{ generate_cast(columns) }},
        {{ audit_columns() }}

    from source

)

select * from raw
