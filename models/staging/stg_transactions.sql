-- Staging model for raw transactions with basic cleaning
-- removes duplicate transaction IDs keeping first occurrence by datetime

with source as (
    select * from {{ source('dojo_take_home', 'transactions') }}
),

cleaned as (
    select
        transaction_id,
        customer_id,
        transaction_amount,
        transaction_datetime,
        -- adding row number to handle duplicates.
        row_number() over (
            partition by transaction_id 
            order by transaction_datetime
        ) as row_num
    from source
)

select
    transaction_id,
    customer_id,
    transaction_amount,
    transaction_datetime
from cleaned
where row_num = 1  -- I would normally use Qualify in here but postgres doesn't support
