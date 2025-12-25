{{
    config(
        materialized='incremental',
        unique_key='transfer_id',
        incremental_strategy='merge',
        tags=['daily']
    )
}}

SELECT 
    TRY_TO_TIMESTAMP(date_created) AS date_created,
    TRY_TO_TIMESTAMP(date_updated) AS date_updated,
    active,
    transfer_id,
    amount,
    from_account_id,
    reference_id,
    sender_transaction_id
    to_account_id
FROM microhouse_raw.public.transfers
{% if is_incremental() %}
    WHERE TRY_TO_TIMESTAMP(date_updated) > (SELECT MAX(date_updated) FROM {{ this }})
{% endif %}