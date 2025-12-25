{{
    config(
        materialized='table',
        unique_key='transaction_type_id',
        tags=['weekly']
    )
}}

SELECT 
    TRY_TO_TIMESTAMP(date_created) AS date_created,
    TRY_TO_TIMESTAMP(date_updated) AS date_updated,
    active,
    transaction_type_id,
    name,
    code,
    direction,
    description 
FROM microhouse_raw.public.transaction_types
{% if is_incremental() %}
    WHERE TRY_TO_TIMESTAMP(date_updated) > (SELECT MAX(date_updated) FROM {{ this }})
{% endif %}