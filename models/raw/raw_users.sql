{{
    config(
        tags = ['raw']
    )
}}

{% set columns = {
    "id"             : "INT64",
    "first_name"     : "STRING",
    "last_name"      : "STRING",
    "email"          : "STRING",
    "age"            : "INT64",
    "gender"         : "STRING",
    "city"           : "STRING",
    "state"          : "STRING",
    "country"        : "STRING",
    "postal_code"    : "STRING",
    "latitude"       : "FLOAT64",
    "longitude"      : "FLOAT64",
    "traffic_source" : "STRING",
    "created_at"     : "TIMESTAMP"
} %}

with source as (

    select * from {{ source('thelook_ecommerce', 'users') }}

),

raw as (

    select
        {{ generate_cast(columns) }},
        {{ audit_columns() }}

    from source

)

select * from raw
