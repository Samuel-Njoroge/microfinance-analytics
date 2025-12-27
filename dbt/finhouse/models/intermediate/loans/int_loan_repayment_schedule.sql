{{
    config(
    materialized='incremental',
    incremental_strategy='merge',
    tags=['daily']
    )
}}

WITH repayment_summary AS (
    SELECT
        loan_id,
        COUNT(*) AS actual_payment_count,
        SUM(amount) AS total_repaid,
        MIN(payment_date) AS first_payment_date,
        MAX(payment_date) AS last_payment_date,
        AVG(amount) AS avg_payment_amount,
        STDDEV(amount) AS stddev_payment_amount
    FROM {{ ref('stg_loan_repayments') }}
    WHERE active = true
    GROUP BY loan_id
)

SELECT
    l.loan_id,
    l.user_id,
    u.first_name || ' ' || u.last_name AS customer_name,
    l.approved_amount,
    l.repayment_amount AS expected_payment_amount,
    l.repayment_months AS expected_payment_count,
    l.balance AS outstanding_balance,
    l.disbursed_date,
    l.due_date,
    l.status,
    
    -- Repayment actuals
    COALESCE(rs.actual_payment_count, 0) AS actual_payment_count,
    COALESCE(rs.total_repaid, 0) AS total_repaid,
    rs.first_payment_date,
    rs.last_payment_date,
    COALESCE(rs.avg_payment_amount, 0) AS avg_payment_amount,
    COALESCE(rs.stddev_payment_amount, 0) AS payment_consistency,
    
    -- Schedule adherence
    l.repayment_months - COALESCE(rs.actual_payment_count, 0) AS remaining_payments,
    CASE 
        WHEN l.repayment_months > 0 
        THEN (COALESCE(rs.actual_payment_count, 0)::float / l.repayment_months) * 100 
        ELSE 0 
    END AS payment_schedule_adherence_pct,
    
    DATEDIFF('day', rs.last_payment_date, CURRENT_DATE) AS days_since_last_payment,
    
    -- Expected vs actual
    l.repayment_amount * l.repayment_months AS expected_total_repayment,
    (l.repayment_amount * l.repayment_months) - COALESCE(rs.total_repaid, 0) AS repayment_variance

FROM {{ ref('stg_loans') }} l
LEFT JOIN {{ ref('stg_users') }} u 
    ON l.user_id = u.user_id
LEFT JOIN repayment_summary rs 
    ON l.loan_id = rs.loan_id
WHERE l.active = true