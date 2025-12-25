{{
    config(
        materialized='incremental',
        unique_key='investment_id',
        tags=['daily']
    )
}}

SELECT
    date_created,
    date_updated,
    active,
    investment_id,
    start_date,
    end_date,
    amount,
    status,
    total_return,
    user_id,
    investment_product_id
FROM microhouse_raw.public.investments
{% if is_incremental() %}
    WHERE TRY_TO_TIMESTAMP(date_updated) > (SELECT MAX(date_updated) FROM {{ this }})
{% endif %}