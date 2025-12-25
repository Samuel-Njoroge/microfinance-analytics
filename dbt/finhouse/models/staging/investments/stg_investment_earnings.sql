{{
    config(
        materialized='incremental',
        unique_key='investment_earning_id',
        incremental_strategy='merge',
        tags=['daily']
    )
}}

SELECT
    TRY_TO_TIMESTAMP(date_created) AS date_created,
    TRY_TO_TIMESTAMP(date_updated) AS date_updated,
    active,
    investment_earning_id,
    amount,
    earning_date,
    investment_id
FROM microhouse_raw.public.investment_earnings
{% if is_incremental() %}
    WHERE TRY_TO_TIMESTAMP(date_updated) > (SELECT MAX(date_updated) FROM {{ this }})
{% endif %}