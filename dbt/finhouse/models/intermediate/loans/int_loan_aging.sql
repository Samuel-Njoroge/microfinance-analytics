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
    l.loan_product_id,
    l.approved_amount,
    l.balance AS outstanding_balance,
    l.disbursed_date,
    l.due_date,
    l.status,
    DATEDIFF('day', l.due_date, CURRENT_DATE) AS days_overdue,
    CASE
        WHEN l.balance = 0 THEN 'PAID_OFF'
        WHEN l.due_date >= CURRENT_DATE THEN 'CURRENT'
        WHEN DATEDIFF('day', l.due_date, CURRENT_DATE) BETWEEN 1 AND 30 THEN '1-30_DAYS'
        WHEN DATEDIFF('day', l.due_date, CURRENT_DATE) BETWEEN 31 AND 60 THEN '31-60_DAYS'
        WHEN DATEDIFF('day', l.due_date, CURRENT_DATE) BETWEEN 61 AND 90 THEN '61-90_DAYS'
        WHEN DATEDIFF('day', l.due_date, CURRENT_DATE) BETWEEN 91 AND 180 THEN '91-180_DAYS'
        WHEN DATEDIFF('day', l.due_date, CURRENT_DATE) > 180 THEN 'OVER_180_DAYS'
        ELSE 'CURRENT'
    END AS aging_bucket,
    
    CASE
        WHEN l.balance = 0 THEN false
        WHEN DATEDIFF('day', l.due_date, CURRENT_DATE) > 0 THEN true
        ELSE false
    END AS is_overdue,
    
    CASE
        WHEN DATEDIFF('day', l.due_date, CURRENT_DATE) > 30 THEN true
        ELSE false
    END AS is_par_30,
    
    CASE
        WHEN DATEDIFF('day', l.due_date, CURRENT_DATE) > 90 THEN true
        ELSE false
    END AS is_non_performing,
    
    l.active

FROM {{ ref('stg_loans') }} l
WHERE l.active = true 
    AND l.status IN ('ACTIVE', 'OVERDUE', 'DEFAULTED')