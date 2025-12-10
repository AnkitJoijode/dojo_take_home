-- dimension: customers
-- one row per customer

select
    customer_id,
    country
from {{ ref('stg_customers') }}
