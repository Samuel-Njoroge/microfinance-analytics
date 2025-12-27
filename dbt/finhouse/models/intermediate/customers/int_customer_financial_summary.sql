{{
    config(
    materialized='incremental',
    incremental_strategy='merge',
    tags=['daily']
    )
}}

WITH customer_transactions AS (
    SELECT
        a.user_id,
        COUNT(DISTINCT t.transaction_id) AS total_transactions,
        SUM(CASE WHEN tt.direction = 'CREDIT' THEN t.amount ELSE 0 END) AS total_credits,
        SUM(CASE WHEN tt.direction = 'DEBIT' THEN t.amount ELSE 0 END) AS total_debits,
        MAX(t.date_created) AS last_transaction_date
    FROM {{ ref('stg_transactions') }} t
    JOIN {{ ref('stg_accounts') }} a 
        ON t.account_id = a.account_id 
    JOIN {{ ref('stg_transaction_types') }} tt 
        ON t.transaction_type_id = tt.transaction_type_id
    WHERE t.active = true
    GROUP BY a.user_id
),

customer_payments AS (
    SELECT
        user_id, 
        COUNT(DISTINCT payment_id) AS total_payments,
        SUM(amount) AS total_payment_amount,
        SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) AS completed_payment_amount,
        SUM(CASE WHEN status = 'PENDING' THEN amount ELSE 0 END) AS pending_payment_amount,
        MAX(date_created) AS last_payment_date
    FROM {{ ref('stg_payments') }}
    WHERE active = TRUE 
    GROUP BY user_id

)

SELECT 
    u.user_id,
    u.first_name || ' ' || u.last_name AS customer_name,
    u.email,
    u.phone,
    
    -- Transactions metrics
    COALESCE(ct.total_transactions, 0) AS total_transactions,
    COALESCE(ct.total_credits, 0) AS total_credits,
    COALESCE(ct.total_debits, 0) AS total_debits,
    COALESCE(ct.total_credits, 0) - COALESCE(ct.total_debits, 0) as net_transaction_flow,
    ct.last_transaction_date,
    
    -- Payment metrics
    COALESCE(cp.total_payments, 0) AS total_payments,
    COALESCE(cp.total_payment_amount, 0) AS total_payment_amount,
    COALESCE(cp.completed_payment_amount, 0) AS completed_payment_amount,
    COALESCE(cp.pending_payment_amount, 0) AS pending_payment_amount,
    cp.last_payment_date,
    
    -- Activity indicators
    DATEDIFF('day', ct.last_transaction_date, CURRENT_DATE) AS days_since_last_transaction,
    DATEDIFF('day', u.date_joined, CURRENT_DATE) AS days_as_customer

FROM {{ ref('stg_users') }} u
INNER JOIN customer_transactions ct 
    ON u.user_id = ct.user_id
LEFT JOIN customer_payments cp 
    ON u.user_id = cp.user_id
