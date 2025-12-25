{{
    config(
        materialized='incremental',
        unique_key='loan_repayment_id',
        incremental_strategy='merge',
        tags=['daily']
    )
}}

SELECT 
    TRY_TO_TIMESTAMP(date_created) AS date_created,
    TRY_TO_TIMESTAMP(date_updated) AS date_updated,
    active,
    loan_repayment_id,
    amount,
    payment_date,
    payment_method,
    reference,
    loan_id,
    transaction_id
FROM microhouse_raw.public.loan_repayments
{% if is_incremental() %}
    WHERE TRY_TO_TIMESTAMP(date_updated) > (SELECT MAX(date_updated) FROM {{ this }})
{% endif %}