# realtime-cart-analytics

Pipeline phân tích hành vi giỏ hàng thời gian thực: **Node.js → Kafka (KRaft) → ClickHouse → Grafana**.
Dự án độc lập, dữ liệu do producer mô phỏng các phiên mua hàng.

## Kiến trúc

```
producer.js ──► Kafka (topic cart_events) ──► ClickHouse ──► Grafana
 (phiên mua      bộ đệm / tách biệt          Kafka engine →    dashboard
  hàng giả lập)                              Materialized View  http://localhost:3000
                                             → MergeTree
```

## Chạy

```bash
docker compose up -d          # Kafka + ClickHouse + Grafana (tự chạy clickhouse/init.sql)
cd producer && npm install
node producer.js              # mô phỏng ~2 phiên mới/giây
```

- Dashboard: http://localhost:3000 (xem ẩn danh; quản trị: `admin` / `admin`)
- ClickHouse (DBeaver): `localhost:8123`, user `default`, không mật khẩu

## Mô hình dữ liệu

Bảng `cart_events_storage`: `session_id, user_id, action, product_id, quantity, price, timestamp (UTC)`.
`action` ∈ `VIEW_PRODUCT`, `ADD_TO_CART`, `REMOVE_FROM_CART`, `CHECKOUT`.

View phân tích:
- `v_conversion_funnel`: số phiên và tỷ lệ chuyển đổi từng bước.
- `v_product_abandonment`: tỷ lệ xóa khỏi giỏ / bỏ giỏ theo sản phẩm.

Các truy vấn mẫu nằm trong `clickhouse/analytics.sql`.

## Mô phỏng

- 300 người dùng, mỗi phiên xem 1–6 sản phẩm, 30% thêm vào giỏ, 25% phiên có hàng sẽ thanh toán.
- 5 sản phẩm đắt (`prod_3, 7, 12, 25, 41`) bị xóa khỏi giỏ 70% (so với 15% bình thường) để dashboard có điểm đáng chú ý.

## Dừng / dọn

```bash
docker compose down        # giữ image
docker compose down -v     # xóa luôn dữ liệu
```

> Khi đổi schema trong `init.sql`, cần `docker compose down -v` rồi `up -d` lại, vì init chỉ chạy khi ClickHouse khởi tạo lần đầu.

## Ghi chú kiến trúc

- Kafka có 2 listener: `INTERNAL kafka:19092` (ClickHouse/Grafana trong Docker) và `EXTERNAL localhost:9092` (Node.js trên host).
- Bảng `cart_events_queue` (Kafka engine) → `cart_events_mv` (Materialized View) → `cart_events_storage` (MergeTree).
- ClickHouse đọc Kafka theo lô (~7,5 giây/lần) nên số liệu tăng theo bước, không phải từng sự kiện.
