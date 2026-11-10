const {
  ConfigValidator,
  Kafka,
  KafkaProducer,
  KafkaConsumer,
  RetryManager,
  RetryStore,
  RetryWorker
} = require("@arcadia-man/kafka-retrier");

const { createClient } = require("redis");

const domainModules = [
  require("./consumers/order-created"),
  require("./consumers/payment-processed"),
  require("./consumers/inventory-updated"),
  require("./consumers/shipment-created"),
  require("./consumers/refund-requested"),
  require("./consumers/notification-requested"),
  require("./consumers/customer-updated"),
  require("./consumers/invoice-generated"),
  require("./consumers/order-cancelled"),
  require("./consumers/warehouse-transfer-created")
];

const consumersConfig = domainModules.flatMap(domain => [
  domain.normalConsumer,
  domain.retryConsumer,
  domain.dlqConsumer
].filter(Boolean));

const config = {
  kafka: {
    clientId: "kafka-retrier-advanced-test",
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(",")
  },
  redis: {
    url: process.env.REDIS_URL || "redis://localhost:6379"
  },
  consumers: consumersConfig
};

let redis;
let kafka;
let producer;
let retryWorker;
const consumers = [];
let shuttingDown = false;

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log(`[SHUTDOWN] ${signal}`);

  try {
    if (retryWorker) await retryWorker.stop();

    for (const consumer of consumers) {
      await consumer.disconnect();
    }

    if (producer) await producer.disconnect();
    if (kafka) await kafka.disconnect();

    if (redis?.isOpen) await redis.quit();

    console.log("[SHUTDOWN] Complete");
    process.exit(0);
  } catch (error) {
    console.error("[SHUTDOWN ERROR]", error);
    process.exit(1);
  }
}

async function start() {
  ConfigValidator.validate(config);

  console.log(`[CONFIG] ${consumersConfig.length} consumers configured`);

  redis = createClient({ url: config.redis.url });
  redis.on("error", error => console.error("[REDIS ERROR]", error));
  await redis.connect();
  console.log("[CONNECTED] Redis");

  kafka = new Kafka(config.kafka);

  producer = new KafkaProducer(kafka);
  await producer.connect();
  console.log("[CONNECTED] Kafka producer");

  const retryStore = new RetryStore(redis);
  retryWorker = new RetryWorker(retryStore, producer);
  await retryWorker.start();
  console.log("[STARTED] Retry worker");

  for (const consumerConfig of consumersConfig) {
    const retryManager = consumerConfig.stage === "dlq"
      ? null
      : new RetryManager({
          retry: consumerConfig.retry,
          dlq: consumerConfig.dlq
        }, retryStore);

    const consumer = new KafkaConsumer(
      kafka,
      consumerConfig,
      retryManager
    );

    await consumer.connect();
    consumers.push(consumer);
  }

  for (let i = 0; i < consumers.length; i++) {
    await consumers[i].start();
    console.log(`[STARTED] ${consumersConfig[i].name}`);
  }

  console.log("[READY] All configured consumers started");
}

process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));

start().catch(async error => {
  console.error("[FATAL]", error);
  await shutdown("STARTUP_FAILURE");
});
