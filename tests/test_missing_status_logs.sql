-- test: detect transactions without any status logs

select
    t.transaction_id,
    t.transaction_datetime,
    t.customer_id,
    'no status logs - orphan transaction' as issue
from {{ ref('fct_transactions') }} t
left join {{ ref('fct_transaction_status_log') }} tsl 
    on t.transaction_id = tsl.transaction_id
where tsl.transaction_id is null
