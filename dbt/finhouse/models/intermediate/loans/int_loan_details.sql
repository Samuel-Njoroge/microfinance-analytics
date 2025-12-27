{{
    config(
    materialized='incremental',
    incremental_strategy='merge',
    tags=['daily']
    )
}}

SELECT
    l.loan_id,
    l.user_id,
    u.first_name || ' ' || u.last_name as customer_name,
    u.email,
    u.phone,
    u.role AS customer_role,
    l.loan_product_id,
    lp.name AS loan_product_name,
    lp.description AS product_description,
    lp.term_months AS product_term_months,
    l.principal_amount,
    l.approved_amount,
    l.interest_rate,
    l.repayment_amount,
    l.repayment_months,
    l.status,
    l.application_date,
    l.approved_date,
    l.disbursed_date,
    l.due_date,
    l.total_paid,
    l.balance AS outstanding_balance,
    l.approved_amount - l.balance AS amount_paid,
    
    -- Calculated fields
    CASE 
        WHEN l.approved_amount > 0 
        THEN (l.total_paid / l.approved_amount) * 100 
        ELSE 0 
    END AS repayment_percentage,
    
    DATEDIFF('day', l.disbursed_date, CURRENT_DATE) AS days_since_disbursement,
    DATEDIFF('day', CURRENT_DATE, l.due_date) AS days_to_maturity,
    DATEDIFF('month', l.disbursed_date, CURRENT_DATE) AS months_since_disbursement,
    
    CASE
        WHEN l.due_date < CURRENT_DATE AND l.balance > 0 THEN 'OVERDUE'
        WHEN l.status = 'ACTIVE' AND l.balance > 0 THEN 'CURRENT'
        WHEN l.balance = 0 THEN 'PAID_OFF'
        ELSE l.status
    END AS loan_status_detailed,
    
    l.date_created,
    l.date_updated,
    l.active

FROM {{ ref('stg_loans') }} l
LEFT JOIN {{ ref('stg_users') }} u 
    ON l.user_id = u.user_id
LEFT JOIN {{ ref('stg_loan_products') }} lp 
    ON l.loan_product_id = lp.loan_product_id
WHERE l.active = true