-- report: system efficiency
-- percentage of approved transactions settled without being held

with approved_settled_transactions as (
    select
        transaction_id,
        was_held
    from {{ ref('fct_transactions') }}
    where approved_datetime is not null 
        and settled_datetime is not null
)

select
    count(*) as total_approved_settled_transactions,
    count(*) filter (where not was_held) as efficient_transactions,
    count(*) filter (where was_held) as held_transactions,
    
    -- efficiency percentage (higher is better)
    round(
        (count(*) filter (where not was_held)::numeric / count(*) * 100), 
        2
    ) as efficiency_percentage,
    
    -- hold rate (lower is better)
    round(
        (count(*) filter (where was_held)::numeric / count(*) * 100), 
        2
    ) as hold_rate_percentage
from approved_settled_transactions
