const { Kafka } = require("kafkajs");

const kafka = new Kafka({ clientId: "order-seeder", brokers: ["localhost:9092"] });
const producer = kafka.producer();

async function seed() {
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
    await producer.disconnect();
    process.exit(1);
});