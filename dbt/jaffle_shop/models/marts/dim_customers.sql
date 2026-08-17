select
    c.customer_id,
    c.first_name,
    c.last_name,
    count(o.order_id) as number_of_orders,
    sum(p.amount) as lifetime_value
from {{ ref('stg_customers') }} c
left join {{ ref('int_customer_orders') }} o
    on c.customer_id = o.customer_id
left join {{ ref('stg_payments') }} p   
    on o.order_id = p.order_id
group by 1, 2, 3