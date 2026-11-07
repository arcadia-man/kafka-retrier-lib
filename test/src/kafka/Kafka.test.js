const test = require("node:test");
const assert = require("node:assert/strict");

const Kafka = require("../../../src/kafka/kafka");
function createConfig() {
    return {
        clientId: "test-service",
        brokers: ["localhost:9092"]
    };
}

test.beforeEach(() => {
    Kafka.instance = null;
});

test.afterEach(() => {
    Kafka.instance = null;
});

test("requires Kafka brokers", () => {
    assert.throws(() => new Kafka({}), { message: "Kafka brokers are required" });
});

test("creates Kafka instance", () => {
    const kafka = new Kafka(createConfig());

    assert.ok(kafka.client);
    assert.equal(Kafka.getInstance(), kafka);
});

test("returns singleton instance through getInstance", () => {
    const kafka = new Kafka(createConfig());

    assert.equal(Kafka.getInstance(), kafka);
});

test("throws when getInstance is called before initialization", () => {
    assert.throws(() => Kafka.getInstance(), {
        message: "Kafka has not been initialized. Create the Kafka instance first."
    });
});

test("prevents creating multiple Kafka instances", () => {
    new Kafka(createConfig());

    assert.throws(() => new Kafka(createConfig()), {
        message: "Kafka instance already exists. Use Kafka.getInstance() instead."
    });
});

test("creates shared producer", () => {
    const kafka = new Kafka(createConfig());

    const producer1 = kafka.createProducer();
    const producer2 = kafka.createProducer();

    assert.equal(producer1, producer2);
    assert.equal(kafka.producer, producer1);
});

test("creates separate consumers", () => {
    const kafka = new Kafka(createConfig());

    const consumer1 = kafka.createConsumer({ groupId: "group-1" });
    const consumer2 = kafka.createConsumer({ groupId: "group-2" });

    assert.notEqual(consumer1, consumer2);
    assert.equal(kafka.consumers.size, 2);
});

test("requires consumer groupId", () => {
    const kafka = new Kafka(createConfig());

    assert.throws(() => kafka.createConsumer({}), {
        message: "Kafka consumer groupId is required"
    });
});

test("requires consumer groupId to be a string", () => {
    const kafka = new Kafka(createConfig());

    assert.throws(() => kafka.createConsumer({ groupId: 123 }), {
        message: "Kafka consumer groupId is required"
    });
});

test("disconnect resets Kafka singleton", async () => {
    const kafka = new Kafka(createConfig());

    await kafka.disconnect();

    assert.equal(Kafka.instance, null);
    assert.throws(() => Kafka.getInstance(), {
        message: "Kafka has not been initialized. Create the Kafka instance first."
    });
});

test("disconnects all consumers and clears consumer set", async () => {
    const kafka = new Kafka(createConfig());

    const consumer1 = kafka.createConsumer({ groupId: "group-1" });
    const consumer2 = kafka.createConsumer({ groupId: "group-2" });

    let consumer1Disconnected = false;
    let consumer2Disconnected = false;

    consumer1.disconnect = async () => {
        consumer1Disconnected = true;
    };

    consumer2.disconnect = async () => {
        consumer2Disconnected = true;
    };

    await kafka.disconnect();

    assert.equal(consumer1Disconnected, true);
    assert.equal(consumer2Disconnected, true);
    assert.equal(kafka.consumers.size, 0);
});

test("disconnects producer and clears producer reference", async () => {
    const kafka = new Kafka(createConfig());

    const producer = kafka.createProducer();

    let producerDisconnected = false;

    producer.disconnect = async () => {
        producerDisconnected = true;
    };

    await kafka.disconnect();

    assert.equal(producerDisconnected, true);
    assert.equal(kafka.producer, null);
});

test("disconnect is safe without producer or consumers", async () => {
    const kafka = new Kafka(createConfig());

    await assert.doesNotReject(() => kafka.disconnect());

    assert.equal(Kafka.instance, null);
});