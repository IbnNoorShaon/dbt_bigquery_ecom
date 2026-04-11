{{
    config(
        materialized = 'view',
        schema = 'serve',
        tags = ['serve', 'batch_run']
    )
}}

select
    -- partition key: first day of current month
    date_trunc(current_date(), month)           as partition_key,

    -- exact timestamp of this run
    current_timestamp()                         as run_datetime,
