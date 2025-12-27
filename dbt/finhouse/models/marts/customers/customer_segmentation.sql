{{
    config(
        materialized='table'
    )
}}

WITH customer_metrics AS (
    SELECT
        cp.user_id,
        cp.first_name || ' ' || cp.last_name AS customer_name,
        cp.email,
        cp.phone,
        cp.is_active,
        cp.is_verified,
        cp.role,
        cp.date_joined,
        cp.last_login,
        
        -- Financial metrics
        cp.total_accounts,
        cp.total_account_balance,
        cp.total_loans,
        cp.active_loans,
        cp.total_loan_amount,
        cp.total_loan_balance,
        cp.total_loan_paid,
        cp.total_investments,
        cp.active_investments,
        cp.total_investment_amount,
        cp.total_investment_returns,
        cp.net_financial_position,
        
        -- Activity metrics
        cfs.total_transactions,
        cfs.total_credits,
        cfs.total_debits,
        cfs.net_transaction_flow,
        cfs.days_since_last_transaction,
        cfs.days_as_customer
        
    FROM {{ ref('int_customer_profiles') }} cp
    LEFT JOIN {{ ref('int_customer_financial_summary') }} cfs ON cp.user_id = cfs.user_id
)

SELECT
    user_id,
    customer_name,
    email,
    phone,
    is_active,
    is_verified,
    role,
    date_joined,
    last_login,
    
    -- Financial metrics
    total_accounts,
    total_account_balance,
    total_loans,
    active_loans,
    total_loan_amount,
    total_loan_balance,
    total_investments,
    total_investment_amount,
    net_financial_position,
    
    -- Activity metrics
    total_transactions,
    days_since_last_transaction,
    days_as_customer,
    
    -- Customer value score (0-100)
    least(100, (
        (CASE WHEN total_account_balance > 0 THEN 20 ELSE 0 END) +
        (CASE WHEN total_loans > 0 THEN 20 ELSE 0 END) +
        (CASE WHEN total_investments > 0 THEN 20 ELSE 0 END) +
        (CASE WHEN total_transactions > 10 THEN 20 ELSE total_transactions * 2 END) +
        (CASE WHEN days_since_last_transaction <= 30 THEN 20 ELSE 0 END)
    )) AS customer_value_score,
    
    -- Customer segment
    CASE
        WHEN total_loans > 0 AND total_investments > 0 AND total_account_balance > 10000 THEN 'PREMIUM'
        WHEN total_loans > 0 AND total_investments > 0 THEN 'HIGH_VALUE'
        WHEN total_loans > 0 or total_investments > 0 THEN 'ACTIVE'
        WHEN total_transactions > 5 THEN 'ENGAGED'
        WHEN days_as_customer <= 90 THEN 'NEW'
        ELSE 'DORMANT'
    END AS customer_segment,
    
    -- Engagement level
    CASE
        WHEN days_since_last_transaction <= 7 THEN 'VERY_ACTIVE'
        WHEN days_since_last_transaction <= 30 THEN 'ACTIVE'
        WHEN days_since_last_transaction <= 90 THEN 'MODERATE'
        ELSE 'INACTIVE'
    END AS engagement_level,
    
    -- Risk profile
    CASE
        WHEN total_loan_balance = 0 AND total_loans > 0 THEN 'LOW_RISK'
        WHEN total_loan_balance > 0 AND total_loan_balance <= total_loan_amount * 0.5 THEN 'LOW_RISK'
        WHEN total_loan_balance > total_loan_amount * 0.5 AND total_loan_balance <= total_loan_amount * 0.8 THEN 'MEDIUM_RISK'
        WHEN total_loan_balance > total_loan_amount * 0.8 THEN 'HIGH_RISK'
        ELSE 'NO_EXPOSURE'
    END AS risk_profile,
    
    current_timestamp AS last_updated
    
FROM customer_metrics
