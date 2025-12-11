-- rpt_system_timeliness.sql (with data quality filter)

with approved_completed_transactions as (
    select
        transaction_id,
        processing_time_seconds,
        processing_time_seconds / 60.0 as processing_time_minutes,
        processing_time_seconds / 3600.0 as processing_time_hours
    from {{ ref('fct_transactions') }}
    where approved_datetime is not null 
        and complete_datetime is not null
        and processing_time_seconds is not null
        and processing_time_seconds > 0  -- exclude negative times
        and processing_time_seconds < 604800  -- exclude > 7 days (likely bad data)
)

select
    count(*) as total_transactions_analyzed,
    
    -- average processing time
    round(avg(processing_time_minutes)::numeric, 2) as avg_processing_time_minutes,
    round(avg(processing_time_hours)::numeric, 2) as avg_processing_time_hours,
    
    -- min/max processing time
    round(min(processing_time_minutes)::numeric, 2) as min_processing_time_minutes,
    round(max(processing_time_minutes)::numeric, 2) as max_processing_time_minutes
from approved_completed_transactions