{{
    config(
        materialized='table'
        
    )
}}

WITH portfolio_metrics AS (
    SELECT
        id.product_name,
        id.product_interest_rate,
        id.duration_months,
        
        COUNT(DISTINCT id.investment_id) as total_investments,
        COUNT(DISTINCT id.user_id) AS unique_investors,
        
        SUM(id.invested_amount) AS total_invested,
        SUM(id.total_return) AS total_returns,
        SUM(id.net_earnings) AS total_earnings,
        
        AVG(id.invested_amount) AS avg_investment_size,
        AVG(id.roi_percentage) AS avg_roi_percentage,
        AVG(id.days_invested) AS avg_days_invested,
        AVG(id.months_invested) AS avg_months_invested,
        
        SUM(CASE WHEN id.investment_status_detailed = 'ACTIVE' THEN id.invested_amount ELSE 0 end) AS active_investments,
        SUM(CASE WHEN id.investment_status_detailed = 'MATURED' THEN id.total_return ELSE 0 end) AS matured_returns,
        SUM(CASE WHEN id.investment_status_detailed = 'PENDING' THEN id.invested_amount ELSE 0 end) AS pending_investments,
        
        COUNT(DISTINCT CASE WHEN id.investment_status_detailed = 'ACTIVE' THEN id.investment_id end) AS active_COUNT,
        COUNT(DISTINCT CASE WHEN id.investment_status_detailed = 'MATURED' THEN id.investment_id end) AS matured_COUNT,
        COUNT(DISTINCT CASE WHEN id.investment_status_detailed = 'PENDING' THEN id.investment_id end) AS pending_count,
        
        -- Performance metrics from investment performance model
        AVG(ip.annualized_return_pct) AS avg_annualized_return,
        AVG(ip.monthly_roi_pct) AS avg_monthly_roi
        
    FROM {{ ref('int_investment_details') }} id
    LEFT JOIN {{ ref('int_investment_performance') }} ip ON id.investment_id = ip.investment_id
    GROUP BY id.product_name, id.product_interest_rate, id.duration_months
)

SELECT
    product_name,
    product_interest_rate,
    duration_months,
    total_investments,
    unique_investors,
    total_invested,
    total_returns,
    total_earnings,
    avg_investment_size,
    avg_roi_percentage,
    avg_annualized_return,
    avg_monthly_roi,
    avg_days_invested,
    avg_months_invested,
    active_investments,
    matured_returns,
    pending_investments,
    active_count,
    matured_count,
    pending_count,
    
    -- Portfolio concentration
    CASE
        WHEN SUM(total_invested) OVER () > 0
        THEN (total_invested / SUM(total_invested) OVER ()) * 100
        ELSE 0
    END AS portfolio_concentration_pct,
    
    -- Performance rating
    CASE
        WHEN avg_annualized_return >= 15 THEN 'EXCELLENT'
        WHEN avg_annualized_return >= 10 THEN 'GOOD'
        WHEN avg_annualized_return >= 5 THEN 'FAIR'
        ELSE 'POOR'
    END AS product_performance_rating,
    
    current_timestamp AS last_updated
    
FROM portfolio_metrics
ORDER BY total_invested DESC
