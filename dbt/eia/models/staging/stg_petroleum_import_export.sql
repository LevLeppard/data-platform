with source as (
    select
        period::date as period_date,
        duoarea,
        "area-name" as area_name,
        product,
        "product-name" as product_name,
        process,
        "process-name" as process_name,
        series,
        "series-description" as series_description,
        value::numeric as value,
        units,
        loaded_at,
        row_number() over (
            partition by period, duoarea, product, process, series
            order by loaded_at desc
        ) as rn
    from {{ source('raw', 'raw_petroleum_import_export') }}
)

select
    period_date,
    duoarea,
    area_name,
    product,
    product_name,
    process,
    process_name,
    series,
    series_description,
    value,
    units,
    loaded_at
from source
where rn = 1