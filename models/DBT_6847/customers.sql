with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

customer_orders as (
    
    select distinct customer_id,
        first_value(a.order_id) over(partition by customer_id order by order_date) first_order, 
        last_value(a.order_id) over(partition by customer_id order by order_date) most_recent_order,
        count(distinct a.order_id) over(partition by customer_id ) total_number_of_orders,
        sum(amount) over(partition by customer_id) total_payment_amount 
    from orders a 
    JOIN payments b using (order_id)
)

select a.customer_id, 
first_name, 
last_name,
first_order,
most_recent_order,
total_number_of_orders,
total_payment_amount from customers a 
JOIN customer_orders b ON a.customer_id = b.customer_id

