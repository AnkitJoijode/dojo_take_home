-- models/staging/stg_customers.sql
-- Staging model for customers with basic cleaning

WITH source AS (
    SELECT * FROM {{ source('dojo_take_home', 'customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        -- Clean country codes (fix typo: 'GBRR' -> 'GBR')
        CASE 
            WHEN UPPER(TRIM(country)) = 'GBRR' THEN 'GBR'
            ELSE UPPER(TRIM(country))
        END as country
    FROM source
)

SELECT DISTINCT
    customer_id,
    country
FROM cleaned