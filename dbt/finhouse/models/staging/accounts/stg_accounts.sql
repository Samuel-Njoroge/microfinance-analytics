{{
    config(
    materialized='incremental',
    unique_key='account_id',
    incremental_strategy='merge',
    tags=['daily']
    )
}}

SELECT
    TRY_TO_TIMESTAMP(date_created) AS date_created,
    TRY_TO_TIMESTAMP(date_updated) AS date_updated,
    active,
    account_id,
    account_number,
    account_type_id,
    balance,
    user_id
FROM microhouse_raw.public.accounts
{% if is_incremental() %}
    WHERE TRY_TO_TIMESTAMP(date_updated) > (SELECT MAX(date_updated) FROM {{ this }})
{% endif %}