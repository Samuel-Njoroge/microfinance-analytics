{{
    config(
        materialized="view",
        tags=['weekly']
    )
}}

SELECT
    id,
    user_id,
    permission_id
FROM microfinance.public.users_user_user_permissions