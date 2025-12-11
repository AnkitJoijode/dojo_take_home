-- detects timestamp anomalies: future dates, dates before business started, or unrealistic dates

select 
    transaction_id,
    'transaction_datetime' as column_name,
    transaction_datetime as invalid_value,
    case 
        when transaction_datetime is null then 'NULL value'
        when transaction_datetime > current_timestamp then 'future date'
        when transaction_datetime < '2020-01-01'::timestamp then 'before business start'
    end as issue
from {{ ref('fct_transactions') }}
where transaction_datetime is null
    or transaction_datetime > current_timestamp
    or transaction_datetime < '2020-01-01'::timestamp

union all

select 
    transaction_id,
    'approved_datetime' as column_name,
    approved_datetime as invalid_value,
    case 
        when approved_datetime > current_timestamp then 'future date'
        when approved_datetime < '2020-01-01'::timestamp then 'before business start'
        when approved_datetime > '2100-01-01'::timestamp then 'unrealistic future (year 2999)'
    end as issue
from {{ ref('fct_transactions') }}
where approved_datetime is not null
    and (approved_datetime > current_timestamp
         or approved_datetime < '2020-01-01'::timestamp)

union all

select 
    transaction_id,
    'complete_datetime' as column_name,
    complete_datetime as invalid_value,
    case 
        when complete_datetime > '2100-01-01'::timestamp then 'unrealistic future (year 2999)'
        when complete_datetime > current_timestamp then 'future date'
        when complete_datetime < '2020-01-01'::timestamp then 'before business start'
    end as issue
from {{ ref('fct_transactions') }}
where complete_datetime is not null
    and (complete_datetime > current_timestamp
         or complete_datetime < '2020-01-01'::timestamp)

union all

select 
    transaction_id,
    'settled_datetime' as column_name,
    settled_datetime as invalid_value,
    case 
        when settled_datetime > current_timestamp then 'future date'
        when settled_datetime < '2020-01-01'::timestamp then 'before business start'
    end as issue
from {{ ref('fct_transactions') }}
where settled_datetime is not null
    and (settled_datetime > current_timestamp
         or settled_datetime < '2020-01-01'::timestamp)