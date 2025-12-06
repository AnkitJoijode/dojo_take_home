with source as (
    select * from {{ source('dojo_take_home', 'customers') }}
),

customers as (
    select
        customer_id,
        -- clean country codes
        case 
            when upper(trim(country)) = 'GBRR' then 'GBR'
            else upper(trim(country))
        end as country
    from source
)

select distinct
    customer_id,
    country
from customers
