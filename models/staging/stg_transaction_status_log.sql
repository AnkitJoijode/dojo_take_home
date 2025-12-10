-- staging layer: normalize status values
-- fixes "completed" typo to "complete"

with source as (
    select * from {{ source('dojo_take_home', 'transaction_status_log') }}
)

select
    transaction_id,
    case 
        when status = 'completed' then 'complete'
        else status
    end as status,
    status_datetime
from source
