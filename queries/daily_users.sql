SELECT id, account_creation_time, email
FROM users
WHERE created_at >= CURRENT_DATE - INTERVAL 1 DAY;