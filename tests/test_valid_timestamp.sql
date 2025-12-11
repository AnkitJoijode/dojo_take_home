
-- checks for null, future dates, and implausible historical dates

{% test valid_timestamp(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is null
    or {{ column_name }} > current_timestamp
    or {{ column_name }} < '2020-01-01'::timestamp

{% endtest %}
