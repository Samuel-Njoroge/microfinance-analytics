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
    u.phone,
    i.investment_product_id,
    ip.name AS product_name,
    ip.description AS product_description,
    ip.interest_rate AS product_interest_rate,
    ip.duration_months,
    ip.min_amount AS product_min_amount,
    ip.max_amount AS product_max_amount,
    i.amount AS invested_amount,
    i.total_return,
    i.start_date,
    i.end_date,
    i.status,
    
    -- Calculated fields
    DATEDIFF('day', i.start_date, CURRENT_DATE) AS days_invested,
    DATEDIFF('day', CURRENT_DATE, i.end_date) AS days_to_maturity,
    DATEDIFF('month', i.start_date, COALESCE(i.end_date, CURRENT_DATE)) AS months_invested,
    
    CASE 
        WHEN i.amount > 0 
        THEN ((i.total_return - i.amount) / i.amount) * 100 
        ELSE 0 
    END AS roi_percentage,
    
    i.total_return - i.amount AS net_earnings,
    
    CASE
        WHEN i.end_date < CURRENT_DATE THEN 'MATURED'
        WHEN i.status = 'ACTIVE' AND i.end_date >= CURRENT_DATE THEN 'ACTIVE'
        WHEN i.status = 'PENDING' THEN 'PENDING'
        ELSE i.status
    END AS investment_status_detailed,
    
    i.date_created,
    i.date_updated,
    i.active

FROM {{ ref('stg_investments') }} i
INNER JOIN {{ ref('stg_investment_products') }} ip 
    ON i.investment_product_id = ip.investment_product_id
INNER JOIN {{ ref('stg_users') }} u 
    ON i.user_id = u.user_id
WHERE i.active = true