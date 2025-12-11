
-- cto = total value of approved transactions

with approved_transactions as (
    select
        t.transaction_id,
        t.transaction_amount,
        c.country
    from {{ ref('fct_transactions') }} t
    inner join {{ ref('dim_customer') }} c on t.customer_id = c.customer_id
    where t.approved_datetime is not null
)

select
    country,
    round(sum(transaction_amount), 2) as total_cto,
    count(distinct transaction_id) as transaction_count,
    round(avg(transaction_amount), 2) as avg_transaction_value,
    round(min(transaction_amount), 2) as min_transaction_value,
    round(max(transaction_amount), 2) as max_transaction_value
from approved_transactions
group by country
order by total_cto desc
