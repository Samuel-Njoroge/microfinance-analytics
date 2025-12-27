{{
    config(
    materialized='incremental',
    incremental_strategy='merge',
    tags=['daily']
    )
}}

WITH customer_loans AS (
    SELECT
        user_id,
        count(DISTINCT loan_id) AS total_loans,
        SUM(CASE when status = 'ACTIVE' then 1 else 0 end) AS active_loans,
        SUM(approved_amount) AS total_loan_amount,
        SUM(balance) AS total_loan_balance,
        SUM(total_paid) AS total_loan_paid
    FROM {{ ref('stg_loans') }}
    WHERE active = true
    GROUP BY user_id
),

customer_investments AS (
    SELECT
        user_id,
        count(DISTINCT investment_id) AS total_investments,
        SUM(CASE when status = 'ACTIVE' then 1 else 0 end) AS active_investments,
        SUM(amount) AS total_investment_amount,
        SUM(total_return) AS total_investment_returns
    FROM {{ ref('stg_investments') }}
    WHERE active = true
    GROUP BY user_id
),

customer_accounts AS (
    SELECT
        user_id,
        count(DISTINCT account_id) AS total_accounts,
        SUM(balance) AS total_account_balance
    FROM {{ ref('stg_accounts') }}
    WHERE active = true
    GROUP BY user_id
)

SELECT
    u.user_id,
    u.username,
    u.first_name,
    u.last_name,
    u.email,
    u.phone,
    u.is_active,
    u.is_verified,
    u.role,
    u.date_joined,
    u.last_login,
    DATEDIFF(day, date_joined, CURRENT_DATE) as days_as_customer,

    -- Account metrics
    COALESCE(ca.total_accounts, 0) AS total_accounts,
    COALESCE(ca.total_account_balance, 0) AS total_account_balance,
    
    -- Loan metrics
    COALESCE(cl.total_loans, 0) AS total_loans,
    COALESCE(cl.active_loans, 0) AS active_loans,
    COALESCE(cl.total_loan_amount, 0) AS total_loan_amount,
    COALESCE(cl.total_loan_balance, 0) AS total_loan_balance,
    COALESCE(cl.total_loan_paid, 0) AS total_loan_paid,
    
    -- Investment metrics
    COALESCE(ci.total_investments, 0) AS total_investments,
    COALESCE(ci.active_investments, 0) AS active_investments,
    COALESCE(ci.total_investment_amount, 0) AS total_investment_amount,
    COALESCE(ci.total_investment_returns, 0) AS total_investment_returns,
    
    -- Overall financial position
    COALESCE(ca.total_account_balance, 0) + 
    COALESCE(ci.total_investment_amount, 0) - 
    COALESCE(cl.total_loan_balance, 0) AS net_financial_position

FROM {{ ref('stg_users') }} u
LEFT JOIN customer_accounts ca 
    ON u.user_id = ca.user_id
LEFT JOIN customer_loans cl  
    ON u.user_id = cl.user_id
LEFT JOIN customer_investments ci 
    ON u.user_id = ci.user_id