{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'inventory_item_id',
        schema = 'serve',
        tags = ['serve', 'fct'],
        partition_by = {
            "field": "partition_key",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by = ['inventory_status', 'product_id', 'distribution_center_id']
    )
}}

with inventory as (

    select * from {{ ref('prep_inventory_items') }}

    {% if is_incremental() %}
        where inventory_created_at > (select max(inventory_created_at) from {{ this }})
    {% endif %}

),

distribution_centers as (

    select
        distribution_center_id,
        distribution_center_name,
        distribution_center_latitude,
        distribution_center_longitude
    from {{ ref('prep_distribution_centers') }}

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
        inventory.inventory_item_id,

        -- foreign keys
        inventory.product_id,
        inventory.distribution_center_id,

        -- product snapshot
        inventory.product_name,
        inventory.product_brand,
        inventory.product_category,
        inventory.product_department,
        inventory.product_sku,
        inventory.product_retail_price,
        inventory.product_cost,
        inventory.gross_margin,

        -- distribution center details (pre-joined for Looker)
        distribution_centers.distribution_center_name,
        distribution_centers.distribution_center_latitude,
        distribution_centers.distribution_center_longitude,

        -- inventory details
        inventory.inventory_status,
        inventory.days_in_inventory,

        -- timestamps
        inventory.inventory_created_at,
        inventory.inventory_sold_at,

        -- derived metrics
        case
            when inventory.days_in_inventory > 90   then 'Slow Moving'
            when inventory.days_in_inventory > 30   then 'Normal'
            else                                         'Fast Moving'
        end                                             as inventory_movement,

        round(safe_divide(
            inventory.gross_margin,
            inventory.product_retail_price
        ) * 100, 2)                                     as margin_pct,

        -- batch run keys
        batch_run.partition_key,
        batch_run.run_datetime

    from inventory
    left join distribution_centers
        on inventory.distribution_center_id = distribution_centers.distribution_center_id
    cross join batch_run

)

select * from transformed
