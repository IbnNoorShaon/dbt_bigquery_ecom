{{
    config(
        tags = ['raw']
    )
}}

{% set columns = {
    "id"            : "INT64",
    "user_id"       : "INT64",
    "sequence_number": "INT64",
    "session_id"    : "STRING",
    "created_at"    : "TIMESTAMP",
    "ip_address"    : "STRING",
    "city"          : "STRING",
    "state"         : "STRING",
    "postal_code"   : "STRING",
    "browser"       : "STRING",
    "traffic_source": "STRING",
    "uri"           : "STRING",
    "event_type"    : "STRING"
} %}

with source as (

    select * from {{ source('thelook_ecommerce', 'events') }}

),

raw as (

    select
        {{ generate_cast(columns) }},
        {{ audit_columns() }}

    from source

)

select * from raw
