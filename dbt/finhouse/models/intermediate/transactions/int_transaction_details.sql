{{
    config(
    materialized='incremental',
    incremental_strategy='merge',
    tags=['daily']
    )
}}

SELECT
    t.transaction_id,
    t.account_id,
    t.transaction_type_id,
    tt.name AS transaction_type_name,
    tt.code AS transaction_type_code,
    tt.direction AS transaction_direction,
    tt.description AS transaction_type_description,
    t.amount,
    t.description AS transaction_description,
    t.date_created AS transaction_date,
    date_trunc('month', t.date_created) AS transaction_month,
    date_trunc('day', t.date_created) AS transaction_day,
    
    a.account_number,
    a.account_type_id,
    a.user_id,
    u.first_name || ' ' || u.last_name AS customer_name,
    u.email,
    
    -- Signed amount for balance calculations
    CASE 
        WHEN tt.direction = 'CREDIT' THEN t.amount 
        WHEN tt.direction = 'DEBIT' THEN -t.amount 
        ELSE 0
    END AS signed_amount,
    
    t.active

FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('stg_transaction_types') }} tt 
    ON t.transaction_type_id = tt.transaction_type_id
LEFT JOIN {{ ref('stg_accounts') }} a 
    ON t.account_id = a.account_id
LEFT JOIN {{ ref('stg_users') }} u 
    ON a.user_id = u.user_id
WHERE t.active = true