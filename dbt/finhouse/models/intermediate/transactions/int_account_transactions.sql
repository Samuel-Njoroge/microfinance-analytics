{{
    config(
    materialized='incremental',
    incremental_strategy='merge',
    tags=['daily']
    )
}}

WITH account_txn_summary AS (
    SELECT
        a.account_id,
        COUNT(DISTINCT t.transaction_id) AS total_transactions,
        SUM(CASE WHEN tt.direction = 'CREDIT' THEN t.amount ELSE 0 END) AS total_credits,
        SUM(CASE WHEN tt.direction = 'DEBIT' THEN t.amount ELSE 0 END) AS total_debits,
        SUM(CASE WHEN tt.direction = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count,
        SUM(CASE WHEN tt.direction = 'DEBIT' THEN 1 ELSE 0 END) AS debit_count,
        MIN(t.date_created) AS first_transaction_date,
        MAX(t.date_created) AS last_transaction_date,
        AVG(t.amount) AS avg_transaction_amount
    FROM {{ ref('stg_accounts') }} a
    LEFT JOIN {{ ref('stg_transactions') }} t 
        ON a.account_id = t.account_id AND t.active = true
    LEFT JOIN {{ ref('stg_transaction_types') }} tt 
        ON t.transaction_type_id = tt.transaction_type_id
    WHERE a.active = true
    GROUP BY a.account_id
)

SELECT
    a.account_id,
    a.account_number,
    a.user_id,
    a.account_type_id,
    at.name AS account_type_name,
    a.balance AS current_balance,
    
    COALESCE(ats.total_transactions, 0) AS total_transactions,
    COALESCE(ats.total_credits, 0) AS total_credits,
    COALESCE(ats.total_debits, 0) AS total_debits,
    COALESCE(ats.credit_count, 0) AS credit_transaction_count,
    COALESCE(ats.debit_count, 0) AS debit_transaction_count,
    
    COALESCE(ats.total_credits, 0) - COALESCE(ats.total_debits, 0) AS net_transaction_flow,
    
    ats.first_transaction_date,
    ats.last_transaction_date,
    COALESCE(ats.avg_transaction_amount, 0) AS avg_transaction_amount,
    
    DATEDIFF('day', ats.last_transaction_date, CURRENT_DATE) AS days_since_last_transaction,
    DATEDIFF('day', a.date_created, CURRENT_DATE) as account_age_days

FROM {{ ref('stg_accounts') }} a
LEFT JOIN {{ ref('stg_account_types') }} at 
    ON a.account_type_id = at.account_type_id
LEFT JOIN account_txn_SUMmary ats 
    ON a.account_id = ats.account_id
WHERE a.active = true