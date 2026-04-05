{% set columns = {
    "id"           : "INT64",
    "name"         : "STRING",
    "cost"         : "FLOAT64",
    "retail_price" : "FLOAT64"
} %}

select
    {{ generate_cast(columns) }},
    {{ audit_columns() }}

from `bigquery-public-data.thelook_ecommerce.products`
limit 5
