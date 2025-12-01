{{
    config(
        materialized="view",
        tags=['weekly']
    )
}}

WITH all_users AS (
    SELECT
        id AS user_id,
        user_name,
        first_name,
        last_name,
        email,
        phone,
        is_active,
        is_staff,
        date_joined,
        last_login
    FROM microfinance.public.users_user
)

SELECT
    *
FROM all_users