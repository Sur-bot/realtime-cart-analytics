-- 1. Bảng Queue: "ống hút" cắm vào Kafka
CREATE TABLE IF NOT EXISTS cart_events_queue (
    session_id String,
    user_id String,
    action String,
    product_id String,
    quantity UInt16,
    price UInt32,
    timestamp DateTime
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:19092',
         kafka_topic_list = 'cart_events',
         kafka_group_name = 'clickhouse_consumer_group',
         kafka_format = 'JSONEachRow',
         date_time_input_format = 'best_effort';

-- 2. Bảng lưu trữ dài hạn
CREATE TABLE IF NOT EXISTS cart_events_storage (
    session_id String,
    user_id String,
    action String,
    product_id String,
    quantity UInt16,
    price UInt32,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (timestamp, action);

-- 3. Materialized View: tự động đổ dữ liệu từ queue sang storage
CREATE MATERIALIZED VIEW IF NOT EXISTS cart_events_mv TO cart_events_storage AS
SELECT * FROM cart_events_queue;

-- 4. View phân tích: phễu chuyển đổi (số phiên ở từng bước)
CREATE VIEW IF NOT EXISTS v_conversion_funnel AS
SELECT
    uniqExactIf(session_id, action = 'VIEW_PRODUCT') AS viewed,
    uniqExactIf(session_id, action = 'ADD_TO_CART')  AS added_to_cart,
    uniqExactIf(session_id, action = 'CHECKOUT')     AS checked_out,
    round(added_to_cart * 100 / nullIf(viewed, 0), 2)         AS view_to_cart_pct,
    round(checked_out * 100 / nullIf(added_to_cart, 0), 2)    AS cart_to_checkout_pct,
    round(checked_out * 100 / nullIf(viewed, 0), 2)           AS view_to_checkout_pct
FROM cart_events_storage;

-- 5. View phân tích: sản phẩm bị bỏ giỏ (thêm vào nhưng không mua)
CREATE VIEW IF NOT EXISTS v_product_abandonment AS
SELECT
    product_id,
    countIf(action = 'ADD_TO_CART')      AS adds,
    countIf(action = 'REMOVE_FROM_CART') AS removes,
    countIf(action = 'CHECKOUT')         AS purchases,
    round(removes * 100 / nullIf(adds, 0), 1)             AS remove_rate_pct,
    round((adds - purchases) * 100 / nullIf(adds, 0), 1)  AS abandon_rate_pct
FROM cart_events_storage
GROUP BY product_id;
