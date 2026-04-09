{{
    config(
        tags = ['prep']
    )
}}

with source as (

    select * from {{ ref('raw_orders') }}

),

transformed as (

    select
        -- primary key
        order_id,

        -- foreign keys
        user_id,

        -- order details
        status                                              as order_status,
        gender                                              as order_gender,
        num_of_item                                         as order_num_items,

        -- timestamps
        created_at                                          as order_created_at,
        shipped_at                                          as order_shipped_at,
        delivered_at                                        as order_delivered_at,
        returned_at                                         as order_returned_at,

        -- derived date parts
        date(created_at)                                    as order_created_date,
        extract(year from created_at)                       as order_year,
        extract(month from created_at)                      as order_month,
        extract(dayofweek from created_at)                  as order_day_of_week,

        -- business logic
        case
            when status = 'Complete'    then true
            else                             false
        end                                                 as is_completed,

        case
            when status = 'Returned'    then true
            else                             false
        end                                                 as is_returned,

        case
            when status = 'Cancelled'   then true
            else                             false
        end                                                 as is_cancelled,

        timestamp_diff(
            delivered_at,
            created_at,
            day
        )                                                   as days_to_deliver,

        -- audit columns
        {{ audit_columns() }}

    from source

)

select * from transformed
