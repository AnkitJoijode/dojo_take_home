-- validates timestamp columns checks for null, future dates, and implausible historical dates

-- tests/generic/test_valid_timestamp.sql

-- tests/specific/test_invalid_timestamps.sql

-- test for invalid timestamps in fct_transactions

select 
    transaction_id,
    'approved_datetime' as column_name,
    approved_datetime as invalid_value,
    'invalid timestamp' as issue
from {{ ref('fct_transactions') }}
where approved_datetime is null
    or approved_datetime > current_timestamp
    or approved_datetime < '2020-01-01'::timestamp

union all

select 
    transaction_id,
    'complete_datetime' as column_name,
    complete_datetime as invalid_value,
    'invalid timestamp' as issue
from {{ ref('fct_transactions') }}
where complete_datetime is null
    or complete_datetime > current_timestamp
    or complete_datetime < '2020-01-01'::timestamp

union all

select 
    transaction_id,
    'settled_datetime' as column_name,
    settled_datetime as invalid_value,
    'invalid timestamp' as issue
from {{ ref('fct_transactions') }}
where settled_datetime is null
    or settled_datetime > current_timestamp
    or settled_datetime < '2020-01-01'::timestamp