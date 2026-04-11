{{
    config(
        materialized = 'table',
        schema = 'serve',
        tags = ['serve', 'dim']
    )
}}

with batch_run as (

    select
        partition_key,
        run_datetime
    from {{ ref('batch_run') }}

),

date_spine as (

    select
        date_day
    from unnest(
        generate_date_array('2019-01-01', current_date(), interval 1 day)
    ) as date_day

),

transformed as (

    select
        -- primary key
        format_date('%Y%m%d', date_day)             as date_id,

        -- date details
        date_day                                     as full_date,
        extract(year from date_day)                  as year,
        extract(quarter from date_day)               as quarter,
        extract(month from date_day)                 as month,
        extract(week from date_day)                  as week_of_year,
        extract(day from date_day)                   as day_of_month,
        extract(dayofweek from date_day)             as day_of_week,

        -- labels
        format_date('%B', date_day)                  as month_name,
        format_date('%A', date_day)                  as day_name,
        format_date('%Y-%m', date_day)               as year_month,
        concat('Q', extract(quarter from date_day),
               '-', extract(year from date_day))     as quarter_label,

        -- flags
        case
            when extract(dayofweek from date_day)
                in (1, 7)                            then true
            else                                          false
        end                                          as is_weekend,

        case
            when date_day = current_date()           then true
            else                                          false
        end                                          as is_today,

        case
            when date_day <= current_date()          then true
            else                                          false
        end                                          as is_past,

        -- batch run keys
        batch_run.partition_key,
        batch_run.run_datetime

    from date_spine
    cross join batch_run

)

select * from transformed
