{{
    config(
        materialized='table'
    )
}}

SELECT
    date_trunc('month', end_date) AS maturity_month,
    maturity_status,
    
    COUNT(DISTINCT investment_id) AS investments_maturing,
    COUNT(DISTINCT user_id) AS customers_affected,
    
    SUM(invested_amount) AS total_principal_maturing,
    SUM(expected_return) AS total_expected_payout,
    SUM(expected_profit) AS total_expected_profit,
    
    AVG(invested_amount) AS AVG_investment_size,
    AVG(days_to_maturity) AS AVG_days_to_maturity,
    
    -- Cumulative totals
    SUM(SUM(expected_return)) OVER (
        ORDER BY date_trunc('month', end_date)
        rows between unbounded preceding and current row
    ) AS cumulative_payouts,
    
    current_timestamp AS last_updated
    
FROM {{ ref('int_investment_maturity') }}
WHERE end_date IS NOT NULL
GROUP BY date_trunc('month', end_date), maturity_status
ORDER BY maturity_month, maturity_status
