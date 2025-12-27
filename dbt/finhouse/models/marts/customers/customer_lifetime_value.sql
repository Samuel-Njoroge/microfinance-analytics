{{
    config(
        materialized='table'
    )
}}

WITH customer_revenue AS (
    SELECT
        cp.user_id,
        cp.first_name || ' ' || cp.last_name AS customer_name,
        cp.date_joined,
        cp.days_as_customer,
        
        -- Revenue from loans (interest income)
        cp.total_loan_paid * 0.20 AS estimated_loan_interest_revenue,  -- Assuming 20% interest
        
        -- Revenue from investments (management fees)
        cp.total_investment_amount * 0.02 AS estimated_investment_fees,  -- Assuming 2% fee
        
        -- Total revenue
        (cp.total_loan_paid * 0.20) + 
        (cp.total_investment_amount * 0.02) AS total_estimated_revenue,
        
        -- Customer metrics
        cp.total_accounts,
        cp.total_loans,
        cp.total_investments,
        cp.net_financial_position
        
    FROM {{ ref('int_customer_profiles') }} cp
    JOIN {{ ref('int_customer_financial_summary') }} cfs ON cp.user_id = cfs.user_id
)

SELECT
    user_id,
    customer_name,
    date_joined,
    days_as_customer,
    
    estimated_loan_interest_revenue,
    estimated_investment_fees,
    total_estimated_revenue,
    
    -- Customer Lifetime Value (CLV)
    CASE
        WHEN days_as_customer > 0
        THEN (total_estimated_revenue / days_as_customer) * 365 * 3  -- 3-year projection
        ELSE 0
    END AS customer_lifetime_value,
    
    -- Average revenue per customer per month
    CASE
        WHEN days_as_customer > 30
        THEN total_estimated_revenue / (days_as_customer / 30.0)
        ELSE 0
    END AS avg_monthly_revenue,
    
    -- Customer metrics
    total_accounts,
    total_loans,
    total_investments,
    net_financial_position,
    
    -- Customer tier based on CLV
    CASE
        WHEN (total_estimated_revenue / nullif(days_as_customer, 0)) * 365 * 3 >= 10000 THEN 'PLATINUM'
        WHEN (total_estimated_revenue / nullif(days_as_customer, 0)) * 365 * 3 >= 5000 THEN 'GOLD'
        WHEN (total_estimated_revenue / nullif(days_as_customer, 0)) * 365 * 3 >= 1000 THEN 'SILVER'
        ELSE 'BRONZE'
    END AS customer_tier,
    
    current_timestamp AS last_updated
    
FROM customer_revenue


