{{
    config(
        materialized='table',
        tags=['weekly']
    )
}}
SELECT 
    date_created,
    date_updated,
    active,
    loan_product_id,
    name,
    description,
    interest_rate,
    term_months,
    min_amount,
    max_amount
FROM microhouse_raw.public.loan_products