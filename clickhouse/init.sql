-- 1. Bảng Queue: "ống hút" cắm vào Kafka
CREATE TABLE IF NOT EXISTS cart_events_queue (
    user_id String,
    action String,
    product_id String,
    timestamp DateTime
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:19092',
         kafka_topic_list = 'cart_events',
         kafka_group_name = 'clickhouse_consumer_group',
         kafka_format = 'JSONEachRow',
         date_time_input_format = 'best_effort';

-- 2. Bảng lưu trữ dài hạn
CREATE TABLE IF NOT EXISTS cart_events_storage (
    user_id String,
    action String,
    product_id String,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (timestamp, action);

-- 3. Materialized View: tự động đổ dữ liệu từ queue sang storage
CREATE MATERIALIZED VIEW IF NOT EXISTS cart_events_mv TO cart_events_storage AS
SELECT * FROM cart_events_queue;
