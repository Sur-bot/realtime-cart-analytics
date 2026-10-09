# realtime-cart-analytics

Pipeline phân tích hành vi giỏ hàng thời gian thực: **Node.js → Kafka (KRaft) → ClickHouse**.

## Chạy

```bash
docker compose up -d          # Kafka + ClickHouse (tự chạy clickhouse/init.sql)
cd producer && npm install
node producer.js              # bắn 1 sự kiện/giây vào topic cart_events
```

## Phân tích

```bash
docker exec rca-clickhouse clickhouse-client --query \
  "SELECT action, count(*) AS total FROM cart_events_storage GROUP BY action"
```

DBeaver: `localhost:8123`, user `default`, không mật khẩu.

## Dừng

```bash
docker compose down        # giữ nguyên image
```

## Ghi chú kiến trúc

- Kafka có 2 listener: `INTERNAL kafka:19092` (ClickHouse trong Docker) và `EXTERNAL localhost:9092` (Node.js trên host).
- Bảng `cart_events_queue` (Kafka engine) → `cart_events_mv` (Materialized View) → `cart_events_storage` (MergeTree).
