const { Kafka } = require("kafkajs");

const kafka = new Kafka({ clientId: "order-seeder", brokers: ["localhost:9092"] });
const admin = kafka.admin();
const producer = kafka.producer();

async function seed() {
    await admin.connect();
    const existing = new Set(await admin.listTopics());
    const required = ["order-create", "order-create.retry", "order-create.dlq"];
    const missing = required.filter(t => !existing.has(t));
    if (missing.length > 0) {
        await admin.createTopics({
            topics: missing.map(topic => ({ topic, numPartitions: 1, replicationFactor: 1 })),
            waitForLeaders: true
        });
    }
    await admin.disconnect();

    await producer.connect();

    await producer.send({
        topic: "order-create",
        messages: [
            { key: "ORDER-SUCCESS", value: JSON.stringify({ orderId: "SUCCESS", amount: 100 }) },
            { key: "ORDER-FAIL", value: JSON.stringify({ orderId: "FAIL", amount: 500 }) }
        ]
    });

    console.log("Seeded SUCCESS and FAIL messages");

    await producer.disconnect();
}

seed().catch(async error => {
    console.error("Seeder failed:", error);
    await admin.disconnect().catch(() => {});
    await producer.disconnect().catch(() => {});
    process.exit(1);
});