{{
    config(
        materialized='table',
        tags=['weekly']
    )
}}

SELECT
    id AS user_id,
    username,
    first_name,
    last_name,
    email,
    phone,
    is_active,
    is_staff,
    date_joined,
    last_login,
    password,
    is_superuser,
    role,
    is_verified
FROM microhouse_raw.public.users_user
