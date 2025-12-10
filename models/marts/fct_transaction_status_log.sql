-- fact table: complete status event log
-- grain: one row per status event
-- purpose: audit trail and detailed workflow analysis

{{ config(
    materialized='incremental',
    unique_key=['transaction_id', 'status_sequence'],
    incremental_strategy='append'
) }}

select
    transaction_id,
    status,
    status_datetime,
    row_number() over (partition by transaction_id order by status_datetime) as status_sequence
from {{ ref('stg_transaction_status_log') }}
{% if is_incremental() %}
where status_datetime > (
    select coalesce(max(status_datetime), '2020-01-01'::timestamp) 
    from {{ this }}
)
{% endif %}
