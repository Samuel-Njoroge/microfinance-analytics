{{
    config(
        materialized="view",
        tags=['weekly']
    )
}}

SELECT
    id,
    user_id,
    group_id
FROM microfinance.public.users_user_groups