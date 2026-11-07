const test = require("node:test");
const assert = require("node:assert/strict");

const KafkaProducer = require("../../../src/kafka/KafkaProducer");

function createKafkaMock() {
    const producer = {
        connectCalls: 0,
        disconnectCalls: 0,
        sendCalls: [],

        async connect() {
            this.connectCalls++;
        },

        async send(data) {
            this.sendCalls.push(data);
            return { success: true };
        },

        async disconnect() {
            this.disconnectCalls++;
        }
    };

    return {
        producer,

        createProducerCalls: 0,

        createProducer() {
            this.createProducerCalls++;
            return producer;
        }
    };
}

test("requires Kafka instance", () => {
    assert.throws(() => new KafkaProducer(), { message: "Kafka instance is required" });
});

test("creates producer from Kafka instance", () => {
    const kafka = createKafkaMock();
    const producer = new KafkaProducer(kafka);

    assert.equal(kafka.createProducerCalls, 1);
    assert.equal(producer.producer, kafka.producer);
});

test("connects producer", async () => {
    const kafka = createKafkaMock();
    const producer = new KafkaProducer(kafka);

    await producer.connect();

    assert.equal(kafka.producer.connectCalls, 1);
    assert.equal(producer.connected, true);
});

test("does not connect producer twice", async () => {
    const kafka = createKafkaMock();
    const producer = new KafkaProducer(kafka);

    await producer.connect();
    await producer.connect();

    assert.equal(kafka.producer.connectCalls, 1);
});

test("requires producer topic", async () => {
    const kafka = createKafkaMock();
    const producer = new KafkaProducer(kafka);

    await producer.connect();

    await assert.rejects(() => producer.send({ messages: [{ value: "test" }] }), { message: "Kafka producer topic is required" });
});

test("requires producer messages", async () => {
    const kafka = createKafkaMock();
    const producer = new KafkaProducer(kafka);

    await producer.connect();

    await assert.rejects(() => producer.send({ topic: "test" }), { message: "Kafka producer messages are required" });
});

test("requires at least one producer message", async () => {
    const kafka = createKafkaMock();
    const producer = new KafkaProducer(kafka);

    await producer.connect();

    await assert.rejects(() => producer.send({ topic: "test", messages: [] }), { message: "Kafka producer messages are required" });
});

test("requires producer to be connected before sending", async () => {
    const kafka = createKafkaMock();
    const producer = new KafkaProducer(kafka);

    await assert.rejects(() => producer.send({ topic: "test", messages: [{ value: "hello" }] }), { message: "Kafka producer is not connected. Call connect() first." });
});

test("sends messages to Kafka", async () => {
    const kafka = createKafkaMock();
    const producer = new KafkaProducer(kafka);
    const messages = [{ key: "ORDER-1", value: JSON.stringify({ orderId: "ORDER-1" }) }];

    await producer.connect();

    const result = await producer.send({ topic: "order-create", messages });

    assert.deepEqual(result, { success: true });
    assert.deepEqual(kafka.producer.sendCalls, [{ topic: "order-create", messages }]);
});

test("disconnects producer", async () => {
    const kafka = createKafkaMock();
    const producer = new KafkaProducer(kafka);

    await producer.connect();
    await producer.disconnect();

    assert.equal(kafka.producer.disconnectCalls, 1);
    assert.equal(producer.connected, false);
});

test("does not disconnect producer when already disconnected", async () => {
    const kafka = createKafkaMock();
    const producer = new KafkaProducer(kafka);

    await producer.disconnect();

    assert.equal(kafka.producer.disconnectCalls, 0);
});

test("can reconnect after disconnect", async () => {
    const kafka = createKafkaMock();
    const producer = new KafkaProducer(kafka);

    await producer.connect();
    await producer.disconnect();
    await producer.connect();

    assert.equal(kafka.producer.connectCalls, 2);
    assert.equal(producer.connected, true);
});