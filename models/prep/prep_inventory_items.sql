{{
    config(
        tags = ['prep']
    )
}}

with source as (

    select * from {{ ref('raw_inventory_items') }}

),

transformed as (

    select
        -- primary key
        id                                                  as inventory_item_id,

        -- foreign keys
        product_id,
        product_distribution_center_id                      as distribution_center_id,

        -- product snapshot
        product_name,
        product_brand,
        product_category,
        product_department,
        product_sku,
        product_retail_price,
        cost                                                as product_cost,

        -- timestamps
        created_at                                          as inventory_created_at,
        sold_at                                             as inventory_sold_at,

        -- business logic
        case
            when sold_at is not null    then 'Sold'
            else                             'Available'
        end                                                 as inventory_status,

        timestamp_diff(
            coalesce(sold_at, current_timestamp()),
            created_at,
            day
        )                                                   as days_in_inventory,

        round(product_retail_price - cost, 2)               as gross_margin,

        -- audit columns
        {{ audit_columns() }}

    from source

)

select * from transformed
