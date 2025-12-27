{{
    config(
    materialized='incremental',
    incremental_strategy='merge',
    tags=['daily']
    )
}}

WITH customer_accounts AS (
    SELECT 
        u.user_id,
        u.username,
        u.first_name,
        u.last_name,
        u.email,
        u.phone AS phone_number,
        u.is_active AS customer_active,
        ac.account_id,
        ac.account_number,
        ac.account_type_id,
        ac.balance AS current_balance,
        ac.active AS account_active,
        act.name AS account_type_name,
        act.interest_rate AS account_interest_rate
    FROM {{ ref('stg_accounts') }} ac
    LEFT JOIN {{ ref('stg_account_types') }} act 
        ON ac.account_type_id = act.account_type_id
    LEFT JOIN {{ ref('stg_users') }} u
        ON u.user_id = ac.user_id
)

SELECT * FROM customer_accounts