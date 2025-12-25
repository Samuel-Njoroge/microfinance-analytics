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
    investment_product_id,
    name,
    description,
    interest_rate,
    duration_months,
    min_amount,
    max_amount
FROM microhouse_raw.public.investment_products