-- Chạy trong DBeaver (localhost:8123, user default) hoặc clickhouse-client

-- 1. Tỷ lệ chuyển đổi toàn phễu
SELECT * FROM v_conversion_funnel FORMAT Vertical;

-- 2. Top 10 sản phẩm bị bỏ giỏ nhiều nhất
SELECT product_id, adds, removes, purchases, remove_rate_pct, abandon_rate_pct
FROM v_product_abandonment
WHERE adds >= 5
ORDER BY (adds - purchases) DESC
LIMIT 10;

-- 3. Top 10 sản phẩm có tỷ lệ xóa khỏi giỏ cao nhất
SELECT product_id, adds, removes, remove_rate_pct
FROM v_product_abandonment
WHERE adds >= 5
ORDER BY remove_rate_pct DESC
LIMIT 10;

-- 4. Sự kiện theo phút
SELECT toStartOfMinute(timestamp) AS minute, action, count() AS events
FROM cart_events_storage
GROUP BY minute, action
ORDER BY minute DESC, action
LIMIT 30;

-- 5. Doanh thu theo phút
SELECT toStartOfMinute(timestamp) AS minute, sum(price * quantity) AS revenue_vnd
FROM cart_events_storage
WHERE action = 'CHECKOUT'
GROUP BY minute
ORDER BY minute DESC
LIMIT 10;

-- 6. Phiên có giỏ hàng nhưng chưa thanh toán (ứng viên gửi voucher nhắc mua)
SELECT session_id, any(user_id) AS user_id, max(timestamp) AS last_seen
FROM cart_events_storage
GROUP BY session_id
HAVING countIf(action = 'ADD_TO_CART') > countIf(action = 'REMOVE_FROM_CART')
   AND countIf(action = 'CHECKOUT') = 0
ORDER BY last_seen DESC
LIMIT 20;
