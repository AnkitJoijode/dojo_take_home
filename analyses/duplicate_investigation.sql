with duplicates as (
    select transaction_id
    from {{ ref('fct_transactions') }}
    group by transaction_id
    having count(*) > 1
)

select
    t.transaction_id,
    t.customer_id,
    t.transaction_amount,
    t.transaction_datetime,
    t.final_status,
    case 
        when lead(t.transaction_amount) over (partition by t.transaction_id order by t.transaction_datetime) = t.transaction_amount
             and lead(t.transaction_datetime) over (partition by t.transaction_id order by t.transaction_datetime) = t.transaction_datetime
        then 'exact duplicate'
        else 'id reuse - different transaction'
    end as duplicate_type
from {{ ref('fct_transactions') }} t
where t.transaction_id in (select transaction_id from duplicates)
order by t.transaction_id, t.transaction_datetime