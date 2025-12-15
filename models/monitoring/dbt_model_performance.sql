-- models/monitoring/dbt_model_performance.sql
{{ config(materialized='incremental') }}

WITH model_timings AS (
    SELECT
        invocation_id,
        model_name,
        status,
        execution_time,
        rows_affected,
        current_timestamp as recorded_at
    FROM {{ ref('dbt_run_results') }}
)

SELECT * FROM model_timings
{% if is_incremental() %}
WHERE recorded_at > (SELECT MAX(recorded_at) FROM {{ this }})
{% endif %}