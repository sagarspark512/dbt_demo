  select customer_id, max(total_payment) as high_total_payment from {{ ref('stg_orders') }} 
  JOIN {{ ref('total_payment_for_each_order') }} using (order_id) group by 1