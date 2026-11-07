const { ConfigValidator, Kafka, KafkaProducer, KafkaConsumer, RetryManager, RetryStore, RetryWorker } = require("../../src");
const { createClient } = require("redis");

const config = {
  kafka: {
    clientId: "order-service",
    brokers: ["localhost:9092"],
  },

  redis: {
    url: "redis://localhost:6379",
  },

  consumers: [
    {
      name: "order-create",
      stage: "normal",
      groupId: "order-create-group",
      topic: "order-create",

      retry: {
        enabled: true,
        topic: "order-create.retry",
        strategy: "exponential",
        maxAttempts: 5,
        initialDelayMs: 1000,
        maxDelayMs: 10000,
      },

      dlq: {
        enabled: true,
        topic: "order-create.dlq",
      },

      handler: async (message) => {
        console.log("[NORMAL]", message);

        if (message.data.orderId === "FAIL") {
          throw new Error("Order processing failed");
        }

        console.log("Order processed:", message.data.orderId);
      },
    },

    {
      name: "order-create-retry",
      stage: "retry",
      groupId: "order-create-retry-group",
      topic: "order-create.retry",

      retry: {
        enabled: true,
        topic: "order-create.retry",
        strategy: "exponential",
        maxAttempts: 5,
        initialDelayMs: 1000,
        maxDelayMs: 10000,
      },

      dlq: {
        enabled: true,
        topic: "order-create.dlq",
      },

      handler: async (message) => {
        console.log("[RETRY]", message);
        console.log("Retry attempt:", message.meta.retry?.attempt);
        throw new Error("Retry processing failed");
      },
    },

    {
      name: "order-create-dlq",
      stage: "dlq",
      groupId: "order-create-dlq-group",
      topic: "order-create.dlq",

      retry: {
        enabled: false,
      },

      dlq: {
        enabled: false,
      },

      handler: async (message) => {
        console.log("[DLQ]", message);
        console.log("Dead letter received:", message.data.orderId);
      },
    },
  ],
};

async function start() {
  ConfigValidator.validate(config);

  const redis = createClient({ url: config.redis.url });

  redis.on("error", (error) => {
    console.error("Redis error:", error);
  });

  await redis.connect();

  const kafka = new Kafka(config.kafka);

  const kafkaProducer = new KafkaProducer(kafka);
  await kafkaProducer.connect();

  const retryStore = new RetryStore(redis);

  const retryWorker = new RetryWorker(retryStore, kafkaProducer);
  await retryWorker.start();

  const consumers = [];

  for (const consumerConfig of config.consumers) {
    let retryManager = null;

    if (consumerConfig.stage !== "dlq") {
      retryManager = new RetryManager({
        retry: consumerConfig.retry,
        dlq: consumerConfig.dlq,
      }, retryStore);
    }

    const consumer = new KafkaConsumer(kafka, consumerConfig, retryManager);

    await consumer.connect();

    consumers.push(consumer);
  }

  for (const consumer of consumers) {
    await consumer.start();
  }

  console.log("Order service started");

  const shutdown = async () => {
    console.log("Shutting down...");

    await retryWorker.stop();

    for (const consumer of consumers) {
      await consumer.disconnect();
    }

    await kafkaProducer.disconnect();
    await kafka.disconnect();
    await redis.quit();

    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

start().catch((error) => {
  console.error("Failed to start service:", error);
  process.exit(1);
});