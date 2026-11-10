const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "kafka-retrier-advanced-seeder",
  brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(",")
});

const admin = kafka.admin();
const producer = kafka.producer();

const PARTITIONS = 3;
const REPLICATION_FACTOR = 1;
const MESSAGES_PER_TOPIC = 20;

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

const runId = `test-${Date.now()}`;

function createMessage(config, index) {
  return {
    id: `${runId}-${config.topic}-${index + 1}`,
    runId,
    topic: config.topic,
    event: config.topic,
    sequence: index + 1,
    createdAt: new Date().toISOString(),
    data: {
      entityId: `${config.topic}-${index + 1}`,
      description: `Integration test message ${index + 1}`,
      amount: (index + 1) * 100,
      status: "CREATED"
    }
  };
}

async function createMissingTopics() {
  console.log("\n========== ENSURE TOPICS EXIST ==========");

  const existing = new Set(await admin.listTopics());
  const missing = allTopics.filter(topic => !existing.has(topic));

  console.log("Existing topics:", existing.size);
  console.log("Missing topics:", missing.length);

  if (missing.length === 0) {
    console.log("All required topics already exist. Nothing to recreate.");
    return;
  }

  await admin.createTopics({
    waitForLeaders: true,
    timeout: 60000,
    topics: missing.map(topic => ({
      topic,
      numPartitions: PARTITIONS,
      replicationFactor: REPLICATION_FACTOR
    }))
  });

  console.log("Created:", missing.join(", "));
}

async function verifyTopics() {
  const existing = new Set(await admin.listTopics());
  const missing = allTopics.filter(topic => !existing.has(topic));

  if (missing.length) {
    throw new Error(`Missing topics: ${missing.join(", ")}`);
  }

  console.log("Verified all", allTopics.length, "required topics.");
}

async function publishMessages(config) {
  const messages = Array.from(
    { length: MESSAGES_PER_TOPIC },
    (_, index) => {
      const payload = createMessage(config, index);

      return {
        key: payload.data.entityId,
        value: JSON.stringify(payload),
        headers: {
          "test-run-id": runId,
          "original-topic": config.topic
        }
      };
    }
  );

  await producer.send({
    topic: config.topic,
    acks: -1,
    timeout: 30000,
    messages
  });

  console.log(
    `[PUBLISHED] ${config.topic}: ${messages.length} messages`
  );

  return messages.length;
}

async function main() {
  let adminConnected = false;
  let producerConnected = false;

  try {
    console.log("========================================");
    console.log("KAFKA RETRIER INTEGRATION TEST SEEDER");
    console.log("Run ID:", runId);
    console.log("========================================");

    await admin.connect();
    adminConnected = true;

    await createMissingTopics();
    await verifyTopics();

    await producer.connect();
    producerConnected = true;

    console.log("\n========== PUBLISH ORIGINAL TOPICS ==========");

    let totalPublished = 0;

    for (const config of topicConfigurations) {
      totalPublished += await publishMessages(config);
    }

    console.log("\n========================================");
    console.log("SEEDING COMPLETED");
    console.log("Required topics:", allTopics.length);
    console.log("Original topics:", topicConfigurations.length);
    console.log("Messages per topic:", MESSAGES_PER_TOPIC);
    console.log("Total messages published:", totalPublished);
    console.log("Run ID:", runId);
    console.log("========================================");
  } catch (error) {
    console.error("\nSEEDING FAILED:", error);
    process.exitCode = 1;
  } finally {
    if (producerConnected) {
      await producer.disconnect().catch(error =>
        console.error("Producer disconnect failed:", error.message)
      );
    }

    if (adminConnected) {
      await admin.disconnect().catch(error =>
        console.error("Admin disconnect failed:", error.message)
      );
    }
  }
}

main();
