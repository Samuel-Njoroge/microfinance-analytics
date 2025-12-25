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
    loan_collateral_id,
    description,
    estimated_value,
    document,
    verified,
    status,
    asset,
    loan_id
FROM microhouse_raw.public.loan_collaterals