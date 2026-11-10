const { Kafka } = require("kafkajs");
const { createClient } = require("redis");
const { clearLog } = require("./logger");

const topicConfigurations = [
  { topic: "order-created", retry: true, dlq: true },
  { topic: "payment-processed", retry: true, dlq: true },
  { topic: "inventory-updated", retry: true, dlq: true },
  { topic: "shipment-created", retry: true, dlq: true },
  { topic: "refund-requested", retry: true, dlq: true },
  { topic: "notification-requested", retry: false, dlq: true },
  { topic: "customer-updated", retry: false, dlq: false },
  { topic: "invoice-generated", retry: true, dlq: true },
  { topic: "order-cancelled", retry: true, dlq: true },
  { topic: "warehouse-transfer-created", retry: true, dlq: true }
];

const allTopics = topicConfigurations.flatMap(config => [
  config.topic,
  ...(config.retry ? [`${config.topic}.retry`] : []),
  ...(config.dlq ? [`${config.topic}.dlq`] : [])
]);

async function resetKafka() {
  console.log("\n========== RESET KAFKA TOPICS ==========");
  const kafka = new Kafka({
    clientId: "kafka-reset-tool",
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(",")
  });

  const admin = kafka.admin();
  try {
    await admin.connect();
    const existing = new Set(await admin.listTopics());
    const toDelete = allTopics.filter(t => existing.has(t));

    if (toDelete.length === 0) {
      console.log("[KAFKA] No existing test topics to delete.");
    } else {
      console.log(`[KAFKA] Deleting ${toDelete.length} test topics...`);
      await admin.deleteTopics({
        topics: toDelete,
        timeout: 30000
      });
      console.log("[KAFKA] Topics deleted successfully.");
      await new Promise(resolve => setTimeout(resolve, 3000));
    }

    console.log(`[KAFKA] Creating fresh ${allTopics.length} test topics...`);
    await admin.createTopics({
      waitForLeaders: true,
      timeout: 60000,
      topics: allTopics.map(topic => ({
        topic,
        numPartitions: 3,
        replicationFactor: 1
      }))
    });
    console.log("[KAFKA] Fresh test topics created successfully.");
  } catch (error) {
    console.error("[KAFKA RESET ERROR]", error.message);
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

async function resetRedis() {
  console.log("\n========== RESET REDIS RETRY STATE ==========");
  const redis = createClient({
    url: process.env.REDIS_URL || "redis://localhost:6379"
  });

  redis.on("error", error => console.error("[REDIS RESET ERROR]", error));

  try {
    await redis.connect();

    let retryKeys = [];
    for await (const item of redis.scanIterator({ MATCH: "retry:*", COUNT: 100 })) {
      if (Array.isArray(item)) retryKeys.push(...item);
      else retryKeys.push(item);
    }

    if (retryKeys.length === 0) {
      console.log("[REDIS] No retry keys found to clean.");
    } else {
      console.log(`[REDIS] Deleting ${retryKeys.length} retry keys...`);
      await redis.del(retryKeys);
      console.log("[REDIS] Cleared all retry keys.");
    }
  } catch (error) {
    console.error("[REDIS RESET ERROR]", error);
  } finally {
    if (redis.isOpen) await redis.quit();
  }
}

function resetLogs() {
  console.log("\n========== RESET LOGS ==========");
  clearLog();
  console.log("[LOGS] Cleared integration-test.log.");
}

async function main() {
  console.log("========================================");
  console.log("ENVIRONMENT RESET TOOL");
  console.log("========================================");

  await resetKafka();
  await resetRedis();
  resetLogs();

  console.log("\n[SUCCESS] Environment reset complete.");
}

if (require.main === module) {
  main().catch(error => {
    console.error("Reset failed:", error);
    process.exit(1);
  });
}

module.exports = { resetKafka, resetRedis, resetLogs };
