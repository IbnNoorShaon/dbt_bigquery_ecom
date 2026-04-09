{{
    config(
        tags = ['prep']
    )
}}

with source as (

    select * from {{ ref('raw_events') }}

),

transformed as (

    select
        -- primary key
        id                                                  as event_id,

        -- foreign keys
        user_id,
        session_id,
        sequence_number,

        -- event details
        event_type,
        uri                                                 as event_uri,
        ip_address,
        browser,
        traffic_source,

        -- location
        city,
        state,
        postal_code,

        -- timestamps
        created_at                                          as event_created_at,
        date(created_at)                                    as event_created_date,
        extract(year from created_at)                       as event_year,
        extract(month from created_at)                      as event_month,
        extract(hour from created_at)                       as event_hour,

        -- business logic
        case
            when event_type = 'purchase'    then true
            else                                 false
        end                                                 as is_purchase,

        case
            when event_type = 'cart'        then true
            else                                 false
        end                                                 as is_cart,

        case
            when event_type = 'home'        then true
            else                                 false
        end                                                 as is_home,

        -- audit columns
        {{ audit_columns() }}

    from source

)

select * from transformed