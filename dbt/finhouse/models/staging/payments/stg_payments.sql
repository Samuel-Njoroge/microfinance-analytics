{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='merge',
        tags=['daily']
    )
}}

SELECT 
    TRY_TO_TIMESTAMP(date_created) AS date_created,
    TRY_TO_TIMESTAMP(date_updated) AS date_updated,
    active,
    payment_id,
    method,
    status,
    amount,
    external_reference,
    description,
    account_id,
    transaction_id,
    user_id 
FROM microhouse_raw.public.payments
{% if is_incremental() %}
    WHERE TRY_TO_TIMESTAMP(date_updated) > (SELECT MAX(date_updated) FROM {{ this }})
{% endif %}