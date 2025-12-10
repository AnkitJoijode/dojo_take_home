-- fct_transactions.sql (incremental version - final working version)

{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

status_pivoted as (
    select
        transaction_id,
        max(case when status = 'approved' then status_datetime end) as approved_datetime,
        max(case when status = 'declined' then status_datetime end) as declined_datetime,
        max(case when status = 'held' then status_datetime end) as held_datetime,
        max(case when status = 'settled' then status_datetime end) as settled_datetime,
        max(case when status = 'complete' then status_datetime end) as complete_datetime,
        max(case when status = 'held' then 1 else 0 end) as was_held_flag
    from {{ ref('stg_transaction_status_log') }}
    group by transaction_id
),

final_status_calc as (
    select
        transaction_id,
        status as final_status,
        row_number() over (partition by transaction_id order by status_datetime desc) as rn
    from {{ ref('stg_transaction_status_log') }}
),

{% if is_incremental() %}

max_last_updated as (
    select greatest(
        coalesce(max(approved_datetime), '2020-01-01'::timestamp),
        coalesce(max(settled_datetime), '2020-01-01'::timestamp),
        coalesce(max(complete_datetime), '2020-01-01'::timestamp),
        coalesce(max(declined_datetime), '2020-01-01'::timestamp),
        coalesce(max(held_datetime), '2020-01-01'::timestamp)
    ) as max_dt
    from {{ this }}
),

transactions_to_update as (
    select distinct tsl.transaction_id 
    from {{ ref('stg_transaction_status_log') }} tsl
    cross join max_last_updated
    where tsl.status_datetime > max_last_updated.max_dt
),

{% endif %}

final as (
    select
        t.transaction_id,
        t.customer_id,
        t.transaction_amount,
        t.transaction_datetime,
        
        -- denormalized status timestamps
        sp.approved_datetime,
        sp.declined_datetime,
        sp.held_datetime,
        sp.settled_datetime,
        sp.complete_datetime,
        
        -- derived flags and metrics
        coalesce(sp.was_held_flag, 0) = 1 as was_held,
        fs.final_status,
        
        -- pre-calculated metrics
        case 
            when sp.approved_datetime is not null and sp.complete_datetime is not null
            then extract(epoch from (sp.complete_datetime - sp.approved_datetime))
            else null
        end as processing_time_seconds,
        
        case 
            when sp.approved_datetime is not null and sp.settled_datetime is not null
            then extract(epoch from (sp.settled_datetime - sp.approved_datetime))
            else null
        end as time_to_settle_seconds,
        
        -- tracking column for incremental loads
        current_timestamp as last_updated_at
        
    from transactions t
    left join status_pivoted sp on t.transaction_id = sp.transaction_id
    left join final_status_calc fs on t.transaction_id = fs.transaction_id and fs.rn = 1
    
    {% if is_incremental() %}
    where t.transaction_id in (select transaction_id from transactions_to_update)
    {% endif %}
)

select * from final