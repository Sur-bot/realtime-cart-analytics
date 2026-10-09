const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'ecommerce-app',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

// ---- Cấu hình mô phỏng ----
const TOPIC = 'cart_events';
const USER_POOL = 300;          // số người dùng khác nhau
const MAX_SESSIONS = 120;       // số phiên chạy đồng thời tối đa
const NEW_SESSION_EVERY_MS = 500;
const P_ADD_AFTER_VIEW = 0.3;   // xem xong thì thêm vào giỏ
const P_CHECKOUT = 0.25;        // có hàng trong giỏ thì thanh toán
const PROBLEM_PRODUCTS = new Set(['prod_3', 'prod_7', 'prod_12', 'prod_25', 'prod_41']);
const P_REMOVE_NORMAL = 0.15;
const P_REMOVE_PROBLEM = 0.7;   // sản phẩm "có vấn đề" (giá cao) hay bị xóa

// 50 sản phẩm, giá cố định (VND). Sản phẩm "có vấn đề" có giá cao.
const PRODUCTS = Array.from({ length: 50 }, (_, i) => {
  const id = `prod_${i}`;
  const price = PROBLEM_PRODUCTS.has(id) ? 2_500_000 : 150_000 + ((i * 37) % 20) * 50_000;
  return { id, price };
});

const rand = (n) => Math.floor(Math.random() * n);
const chance = (p) => Math.random() < p;
const sleep = (min, max) => new Promise((r) => setTimeout(r, min + rand(max - min)));

let activeSessions = 0;
let sentCount = 0;

async function emit(session, action, product, quantity = 1) {
  const event = {
    session_id: session.id,
    user_id: session.userId,
    action,
    product_id: product.id,
    quantity,
    price: product.price,
    timestamp: new Date().toISOString(),
  };
  try {
    await producer.send({ topic: TOPIC, messages: [{ key: session.userId, value: JSON.stringify(event) }] });
    sentCount++;
  } catch (err) {
    console.error('Gửi sự kiện thất bại:', err.message);
  }
}

async function runSession() {
  activeSessions++;
  const session = {
    id: `sess_${Date.now().toString(36)}${rand(1e6).toString(36)}`,
    userId: `user_${rand(USER_POOL)}`,
  };
  const cart = new Map(); // product_id -> { product, quantity }

  try {
    const views = 1 + rand(6);
    for (let i = 0; i < views; i++) {
      const product = PRODUCTS[rand(PRODUCTS.length)];
      await emit(session, 'VIEW_PRODUCT', product);
      await sleep(500, 2500);

      if (chance(P_ADD_AFTER_VIEW)) {
        const quantity = 1 + rand(3);
        await emit(session, 'ADD_TO_CART', product, quantity);
        cart.set(product.id, { product, quantity });
        await sleep(500, 2000);

        const pRemove = PROBLEM_PRODUCTS.has(product.id) ? P_REMOVE_PROBLEM : P_REMOVE_NORMAL;
        if (chance(pRemove)) {
          await emit(session, 'REMOVE_FROM_CART', product, quantity);
          cart.delete(product.id);
          await sleep(300, 1500);
        }
      }
    }

    if (cart.size > 0 && chance(P_CHECKOUT)) {
      for (const { product, quantity } of cart.values()) {
        await emit(session, 'CHECKOUT', product, quantity);
      }
    }
  } finally {
    activeSessions--;
  }
}

async function run() {
  await producer.connect();
  console.log('Đã kết nối Kafka thành công! Bắt đầu mô phỏng các phiên mua hàng...');

  setInterval(() => {
    if (activeSessions < MAX_SESSIONS) runSession().catch(console.error);
  }, NEW_SESSION_EVERY_MS);

  setInterval(() => {
    console.log(`[stats] phiên đang chạy: ${activeSessions} | tổng sự kiện đã gửi: ${sentCount}`);
  }, 5000);
}

process.on('SIGINT', async () => {
  await producer.disconnect();
  process.exit(0);
});

run().catch(console.error);
