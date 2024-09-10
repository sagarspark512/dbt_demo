{{
  config(
    materialized = 'incremental',
    unique_key = 'customer_id',
    merge_update_columns = ['AVG_ORDER_AMOUNT']
  )
}}

select 
    *
from {{ ref('Average_Order_Amount_by_Customer') }}

{% if is_incremental() %}

where customer_id in (select customer_id from {{ this }} )

{% endif %}