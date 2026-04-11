{{
    config(
        materialized = 'table',
        schema = 'serve',
        tags = ['serve', 'dim']
    )
}}

with customers as (

    select * from {{ ref('prep_users') }}

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
        user_id                                             as customer_id,

        -- customer details
        full_name                                           as customer_name,
        first_name                                          as customer_first_name,
        last_name                                           as customer_last_name,
        email                                               as customer_email,
        gender                                              as customer_gender,

        -- demographics
        age                                                 as customer_age,
        age_group                                           as customer_age_group,

        -- location
        city                                                as customer_city,
        state                                               as customer_state,
        country                                             as customer_country,
        postal_code                                         as customer_postal_code,
        latitude                                            as customer_latitude,
        longitude                                           as customer_longitude,

        -- acquisition
        traffic_source                                      as customer_traffic_source,

        -- timestamps
        user_created_at                                     as customer_created_at,
        user_created_date                                   as customer_created_date,
        user_created_year                                   as customer_created_year,

        -- batch run keys
        batch_run.partition_key,
        batch_run.run_datetime

    from customers
    cross join batch_run

)

select * from transformed
