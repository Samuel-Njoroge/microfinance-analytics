{{
    config(
    materialized='incremental',
    incremental_strategy='merge',
    tags=['daily']
    )
}}

SELECT
    i.investment_id,
    i.user_id,
    u.first_name || ' ' || u.last_name AS customer_name,
    u.email,
    i.investment_product_id,
    ip.name AS product_name,
    i.amount AS invested_amount,
    i.total_return AS expected_return,
    i.start_date,
    i.end_date,
    i.status,
    ip.duration_months,
    
    DATEDIFF('day', CURRENT_DATE, i.end_date) AS days_to_maturity,
    DATEDIFF('month', CURRENT_DATE, i.end_date) AS months_to_maturity,
    
    CASE
        WHEN i.end_date < CURRENT_DATE THEN 'MATURED'
        WHEN DATEDIFF('day', CURRENT_DATE, i.end_date) <= 30 THEN 'MATURING_SOON'
        WHEN DATEDIFF('day', CURRENT_DATE, i.end_date) <= 90 THEN 'MATURING_3_MONTHS'
        ELSE 'LONG_TERM'
    END AS maturity_status,
    
    CASE
        WHEN i.end_date < CURRENT_DATE THEN true
        ELSE false
    END AS is_matured,
    
    i.total_return - i.amount AS expected_profit

FROM {{ ref('stg_investments') }} i
JOIN {{ ref('stg_investment_products') }} ip
    ON i.investment_product_id = ip.investment_product_id
JOIN {{ ref('stg_users') }} u
    ON i.user_id = u.user_id
WHERE i.active = true
