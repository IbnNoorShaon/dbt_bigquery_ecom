{{
    config(
        tags = ['prep']
    )
}}

with source as (

    select * from {{ ref('raw_distribution_centers') }}

),

transformed as (

    select
        -- primary key
        id                                                  as distribution_center_id,

        -- details
        name                                                as distribution_center_name,
        latitude                                            as distribution_center_latitude,
        longitude                                           as distribution_center_longitude,

        -- audit columns
        {{ audit_columns() }}

    from source

)

select * from transformed