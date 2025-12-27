{{
    config(
        materialized='table'
    )
}}

WITH monthly_disbursements AS (
    SELECT
        DATE_TRUNC(
            'month',
            TO_DATE(disbursed_date)
        ) AS disbursement_month,

        loan_product_name,

        COUNT(DISTINCT loan_id) AS loans_disbursed,
        COUNT(DISTINCT user_id) AS unique_borrowers,

        SUM(approved_amount) AS total_disbursed,
        AVG(approved_amount) AS avg_loan_size,
        MIN(approved_amount) AS min_loan_size,
        MAX(approved_amount) AS max_loan_size,

        AVG(interest_rate) AS avg_interest_rate,
        AVG(repayment_months) AS avg_term_months

    FROM {{ ref('int_loan_details') }}
    WHERE disbursed_date IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    disbursement_month,
    loan_product_name,
    loans_disbursed,
    unique_borrowers,
    total_disbursed,
    avg_loan_size,
    min_loan_size,
    max_loan_size,
    avg_interest_rate,
    avg_term_months,

    LAG(total_disbursed) OVER (
        PARTITION BY loan_product_name
        ORDER BY disbursement_month
    ) AS prev_month_disbursed,

    CASE
        WHEN LAG(total_disbursed) OVER (
            PARTITION BY loan_product_name
            ORDER BY disbursement_month
        ) > 0
        THEN (
            (total_disbursed - LAG(total_disbursed) OVER (
                PARTITION BY loan_product_name
                ORDER BY disbursement_month
            ))
            / LAG(total_disbursed) OVER (
                PARTITION BY loan_product_name
                ORDER BY disbursement_month
            )
        ) * 100
        ELSE 0
    END AS mom_growth_pct,

    CURRENT_TIMESTAMP AS last_updated

FROM monthly_disbursements
ORDER BY disbursement_month DESC
