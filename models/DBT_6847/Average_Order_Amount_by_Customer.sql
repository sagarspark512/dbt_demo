select a.customer_id, c.first_name, c.last_name, avg(amount) avg_order_amount from {{ ref('stg_orders') }} a
JOIN {{ ref('stg_payments') }} b using (order_id)
JOIN {{ ref('stg_customers') }} c using (customer_id) group by 1, 2, 3