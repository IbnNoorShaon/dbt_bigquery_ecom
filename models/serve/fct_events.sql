{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'event_id',
        schema = 'serve',
        tags = ['serve', 'fct'],
        partition_by = {
            "field": "partition_key",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by = ['event_created_date', 'event_type', 'user_id']
    )
}}

with events as (

    select * from {{ ref('prep_events') }}

    {% if is_incremental() %}
        where event_created_at > (select max(event_created_at) from {{ this }})
    {% endif %}

),

customers as (

    select
        user_id,
        age_group                       as customer_age_group,
        country                         as customer_country,
        traffic_source                  as customer_traffic_source
    from {{ ref('prep_users') }}

),

batch_run as (

    select
        partition_key,
        run_datetime
    from {{ ref('batch_run') }}

),

transformed as (

    select
        -- primary key
        events.event_id,

        -- foreign keys
        events.user_id,
        events.session_id,
        events.sequence_number,

        -- event details
        events.event_type,
        events.event_uri,
        events.browser,
        events.traffic_source,

        -- location
        events.city,
        events.state,
        events.postal_code,

        -- timestamps
        events.event_created_at,
        events.event_created_date,
        events.event_year,
        events.event_month,
        events.event_hour,

        -- event flags
        events.is_purchase,
        events.is_cart,
        events.is_home,

        -- customer details (pre-joined for Looker)
        customers.customer_age_group,
        customers.customer_country,
        customers.customer_traffic_source,

        -- derived metrics
        case
            when events.event_hour between 6 and 11    then 'Morning'
            when events.event_hour between 12 and 17   then 'Afternoon'
            when events.event_hour between 18 and 21   then 'Evening'
            else                                            'Night'
        end                                             as time_of_day,

        -- batch run keys
        batch_run.partition_key,
        batch_run.run_datetime

    from events
    left join customers
        on events.user_id = customers.user_id
    cross join batch_run

)

select * from transformed
