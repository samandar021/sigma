-- View 1: users_extended
CREATE OR REPLACE VIEW users_extended AS
SELECT
    u.id,
    u.name,
    u.email,
    COALESCE(SUM(o.quantity * p.price), 0) AS total_spent,
    COALESCE(COUNT(o.id), 0) AS order_count,
    CASE
        WHEN COUNT(o.id) > 0 THEN AVG(o.quantity * p.price)
        ELSE 0
    END AS avg_spent
FROM users u
LEFT JOIN orders o ON o.user_id = u.id
LEFT JOIN products p ON p.id = o.product_id
GROUP BY u.id, u.name, u.email;

-- View 2: orders_extended
CREATE OR REPLACE VIEW orders_extended AS
SELECT
    o.id,
    o.user_id,
    u.name AS user_name,
    o.product_id,
    p.name AS product_name,
    o.quantity,
    (o.quantity * p.price) AS total_price_per_order
FROM orders o
JOIN users u ON u.id = o.user_id
JOIN products p ON p.id = o.product_id;
