{{
    config(
        tags = ['prep']
    )
}}

with source as (

    select * from {{ ref('raw_users') }}

),

transformed as (

    select
        -- primary key
        id                                                  as user_id,

        -- user details
        first_name,
        last_name,
        concat(first_name, ' ', last_name)                  as full_name,
        email,
        gender,

        -- demographics
        age,
        case
            when age < 18               then 'Under 18'
            when age between 18 and 24  then '18-24'
            when age between 25 and 34  then '25-34'
            when age between 35 and 44  then '35-44'
            when age between 45 and 54  then '45-54'
            else                             '55+'
        end                                                 as age_group,

        -- location
        city,
        state,
        country,
        postal_code,
        latitude,
        longitude,

        -- traffic
        traffic_source,

        -- timestamps
        created_at                                          as user_created_at,
        date(created_at)                                    as user_created_date,
        extract(year from created_at)                       as user_created_year,

        -- audit columns
        {{ audit_columns() }}

    from source

)

select * from transformed
