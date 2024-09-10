{% set payment_methods = ["credit_card", "coupon", "bank_transfer", "gift_card"] %}

select
    stg_orders.order_id,
    customer_id,
    order_date,
    status as order_status,
    {% for payment_method in payment_methods %}
    sum(case when payment_method = '{{payment_method}}'
    then amount else 0 end) as {{payment_method}}_amount,
    {% endfor %}    
    sum(amount) as total_amount
from {{ ref('stg_orders') }}
JOIN  {{ ref('stg_payments') }} using (order_id)
group by 1, 2, 3, 4