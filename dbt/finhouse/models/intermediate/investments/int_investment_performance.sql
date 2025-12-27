{{
    config(
    materialized='incremental',
    incremental_strategy='merge',
    tags=['daily']
    )
}}

WITH investment_earnings AS (
    SELECT
        investment_id,
        COUNT(*) AS earning_count,
        SUM(amount) AS total_earnings,
        MIN(earning_date) AS first_earning_date,
        MAX(earning_date) AS last_earning_date,
        AVG(amount) AS avg_earning_amount
    FROM {{ ref('stg_investment_earnings') }}
    WHERE active = true
    GROUP BY investment_id
)

SELECT
    i.investment_id,
    i.user_id,
    i.investment_product_id,
    i.amount AS principal_amount,
    i.total_return,
    i.start_date,
    i.end_date,
    i.status,
    
    -- Earnings metrics
    COALESCE(ie.earning_count, 0) AS total_earnings_paid,
    COALESCE(ie.total_earnings, 0) AS total_earnings_amount,
    ie.first_earning_date,
    ie.last_earning_date,
    COALESCE(ie.avg_earning_amount, 0) AS avg_earning_amount,
    
    -- Performance calculations
    i.total_return - i.amount AS net_profit,
    
    CASE 
        WHEN i.amount > 0 
        THEN ((i.total_return - i.amount) / i.amount) * 100 
        ELSE 0 
    END AS return_on_investment_pct,
    
    CASE
        WHEN DATEDIFF('month', i.start_date, COALESCE(i.end_date, CURRENT_DATE)) > 0
        THEN (((i.total_return - i.amount) / i.amount) / 
              DATEDIFF('month', i.start_date, COALESCE(i.end_date, CURRENT_DATE))) * 100
        ELSE 0
    END AS monthly_roi_pct,
    
    CASE
        WHEN DATEDIFF('year', i.start_date, COALESCE(i.end_date, CURRENT_DATE)) > 0
        THEN (((i.total_return - i.amount) / i.amount) / 
              DATEDIFF('year', i.start_date, COALESCE(i.end_date, CURRENT_DATE))) * 100
        ELSE 0
    END AS annualized_return_pct,
    
    DATEDIFF('day', ie.last_earning_date, CURRENT_DATE) AS days_since_last_earning

FROM {{ ref('stg_investments') }} i
LEFT JOIN investment_earnings ie 
    ON i.investment_id = ie.investment_id
WHERE i.active = true