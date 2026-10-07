{{
    config(
        materialized='incremental',
        unique_key=['month', 'product_name', 'units']
    )
}}

select
    date_trunc('month', period_date) as month,
    product_name,
    sum(value) as total_value,
    units
from {{ ref('stg_petroleum_consumption') }}
where product_name not like '%Total%'
group by 1, 2, 4