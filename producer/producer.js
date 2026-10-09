const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'ecommerce-app',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();
const ACTIONS = ['ADD_TO_CART', 'REMOVE_FROM_CART', 'CHECKOUT'];

async function run() {
  await producer.connect();
  console.log('Đã kết nối Kafka thành công!');

  setInterval(async () => {
    const event = {
      user_id: `user_${Math.floor(Math.random() * 1000)}`,
      action: ACTIONS[Math.floor(Math.random() * ACTIONS.length)],
      product_id: `prod_${Math.floor(Math.random() * 50)}`,
      timestamp: new Date().toISOString(),
    };

    try {
      await producer.send({
        topic: 'cart_events',
        messages: [{ value: JSON.stringify(event) }],
      });
      console.log(`Đã gửi sự kiện: ${event.action} của ${event.user_id}`);
    } catch (err) {
      console.error('Gửi sự kiện thất bại:', err.message);
    }
  }, 1000);
}

process.on('SIGINT', async () => {
  await producer.disconnect();
  process.exit(0);
});

run().catch(console.error);
