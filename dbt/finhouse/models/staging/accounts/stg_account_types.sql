{{
    config(
        materialized='table',
        tags=['weekly']
    )
}}

SELECT
    TRY_TO_TIMESTAMP(date_created) AS date_created,
    TRY_TO_TIMESTAMP(date_updated) AS date_updated,
    active,
    account_type_id,
    name,
    interest_rate,
    description
FROM microhouse_raw.public.account_types