const test = require("node:test");
const assert = require("node:assert/strict");

const KafkaConsumer = require("../../../src/kafka/KafkaConsumer");

function createKafkaMock() {
    const consumer = {
        connectCalls: 0,
        disconnectCalls: 0,
        subscribeCalls: [],
        runCalls: [],

        async connect() {
            this.connectCalls++;
        },

        async subscribe(config) {
            this.subscribeCalls.push(config);
        },

        async run(config) {
            this.runCalls.push(config);
        },

        async disconnect() {
            this.disconnectCalls++;
        }
    };

    return {
        consumer,
        createConsumerCalls: [],
        createConsumer(config) {
            this.createConsumerCalls.push(config);
            return consumer;
        }
    };
}

function createConfig(overrides = {}) {
    return {
        name: "order-create",
        stage: "normal",
        groupId: "order-create-group",
        topic: "order-create",
        handler: async () => {},
        ...overrides
    };
}

function createKafkaMessage(overrides = {}) {
    return {
        key: Buffer.from("ORDER-1"),
        value: Buffer.from(JSON.stringify({ orderId: "ORDER-1", amount: 100 })),
        offset: "100",
        headers: {
            "message-id": Buffer.from("message-1"),
            source: Buffer.from("order-service")
        },
        ...overrides
    };
}

test("requires Kafka instance", () => {
    assert.throws(() => new KafkaConsumer(null, createConfig()), { message: "Kafka instance is required" });
});

test("requires consumer configuration", () => {
    const kafka = createKafkaMock();

    assert.throws(() => new KafkaConsumer(kafka), { message: "Kafka consumer configuration is required" });
});

test("requires groupId", () => {
    const kafka = createKafkaMock();

    assert.throws(() => new KafkaConsumer(kafka, createConfig({ groupId: "" })), { message: "Kafka consumer groupId is required" });
});

test("requires topic", () => {
    const kafka = createKafkaMock();

    assert.throws(() => new KafkaConsumer(kafka, createConfig({ topic: "" })), { message: "Kafka consumer topic is required" });
});

test("requires handler", () => {
    const kafka = createKafkaMock();

    assert.throws(() => new KafkaConsumer(kafka, createConfig({ handler: null })), { message: "Kafka consumer handler must be a function" });
});

test("creates Kafka consumer with groupId", () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    assert.deepEqual(kafka.createConsumerCalls, [{ groupId: "order-create-group" }]);
    assert.equal(consumer.groupId, "order-create-group");
});

test("connects and subscribes to configured topic", async () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    await consumer.connect();

    assert.equal(kafka.consumer.connectCalls, 1);
    assert.deepEqual(kafka.consumer.subscribeCalls, [{ topic: "order-create", fromBeginning: false }]);
    assert.equal(consumer.connected, true);
});

test("does not connect twice", async () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    await consumer.connect();
    await consumer.connect();

    assert.equal(kafka.consumer.connectCalls, 1);
    assert.equal(kafka.consumer.subscribeCalls.length, 1);
});

test("requires connection before start", async () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    await assert.rejects(() => consumer.start(), { message: 'Kafka consumer "order-create" is not connected. Call connect() first.' });
});

test("starts Kafka consumer", async () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    await consumer.connect();
    await consumer.start();

    assert.equal(kafka.consumer.runCalls.length, 1);
    assert.equal(consumer.running, true);
});

test("does not start consumer twice", async () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    await consumer.connect();
    await consumer.start();
    await consumer.start();

    assert.equal(kafka.consumer.runCalls.length, 1);
});

test("creates KafkaMessage from KafkaJS message", () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    const message = consumer.createKafkaMessage({
        topic: "order-create",
        partition: 2,
        message: createKafkaMessage()
    });

    assert.deepEqual(message.data, { orderId: "ORDER-1", amount: 100 });
    assert.equal(message.meta.messageId, "message-1");
    assert.equal(message.meta.topic, "order-create");
    assert.equal(message.meta.partition, 2);
    assert.equal(message.meta.offset, "100");
    assert.equal(message.meta.key, "ORDER-1");
    assert.equal(message.meta.consumer, "order-create");
    assert.equal(message.meta.stage, "normal");
    assert.deepEqual(message.meta.headers, {
        "message-id": "message-1",
        source: "order-service"
    });
});

test("parses string Kafka message when value is not JSON", () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    const message = consumer.createKafkaMessage({
        topic: "order-create",
        partition: 0,
        message: createKafkaMessage({ value: Buffer.from("plain-text") })
    });

    assert.equal(message.data, "plain-text");
});

test("returns null when Kafka message value is null", () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    const message = consumer.createKafkaMessage({
        topic: "order-create",
        partition: 0,
        message: createKafkaMessage({ value: null })
    });

    assert.equal(message.data, null);
});

test("returns null when Kafka message key is null", () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    const message = consumer.createKafkaMessage({
        topic: "order-create",
        partition: 0,
        message: createKafkaMessage({ key: null })
    });

    assert.equal(message.meta.key, null);
});

test("preserves original metadata from headers", () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    const message = consumer.createKafkaMessage({
        topic: "order-create.retry",
        partition: 2,
        message: createKafkaMessage({
            headers: {
                "message-id": Buffer.from("message-1"),
                "original-topic": Buffer.from("order-create"),
                "original-partition": Buffer.from("1"),
                "original-offset": Buffer.from("50")
            }
        })
    });

    assert.deepEqual(message.meta.original, {
        topic: "order-create",
        partition: 1,
        offset: "50"
    });
});

test("uses current metadata when original headers are absent", () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    const message = consumer.createKafkaMessage({
        topic: "order-create",
        partition: 2,
        message: createKafkaMessage({ headers: {} })
    });

    assert.deepEqual(message.meta.original, {
        topic: "order-create",
        partition: 2,
        offset: "100"
    });
});

test("parses retry metadata", () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig({ stage: "retry" }));

    const message = consumer.createKafkaMessage({
        topic: "order-create.retry",
        partition: 0,
        message: createKafkaMessage({
            headers: {
                "message-id": Buffer.from("message-1"),
                "retry-attempt": Buffer.from("2"),
                "retry-max-attempts": Buffer.from("3"),
                "retry-topic": Buffer.from("order-create.retry"),
                "dlq-topic": Buffer.from("order-create.dlq")
            }
        })
    });

    assert.deepEqual(message.meta.retry, {
        enabled: true,
        attempt: 2,
        maxAttempts: 3,
        retryTopic: "order-create.retry",
        dlqTopic: "order-create.dlq"
    });
});

test("returns null retry metadata when retry attempt is absent", () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    const message = consumer.createKafkaMessage({
        topic: "order-create",
        partition: 0,
        message: createKafkaMessage({ headers: {} })
    });

    assert.equal(message.meta.retry, null);
});

test("supports message-id header fallback", () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    const message = consumer.createKafkaMessage({
        topic: "order-create",
        partition: 0,
        message: createKafkaMessage({
            headers: {
                "message-id": Buffer.from("message-2")
            }
        })
    });

    assert.equal(message.meta.messageId, "message-2");
});

test("calls handler when Kafka consumer receives message", async () => {
    const kafka = createKafkaMock();
    let receivedMessage = null;

    const consumer = new KafkaConsumer(kafka, createConfig({
        handler: async message => {
            receivedMessage = message;
        }
    }));

    await consumer.connect();
    await consumer.start();

    const runConfig = kafka.consumer.runCalls[0];

    await runConfig.eachMessage({
        topic: "order-create",
        partition: 1,
        message: createKafkaMessage()
    });

    assert.deepEqual(receivedMessage.data, {
        orderId: "ORDER-1",
        amount: 100
    });
});

test("passes handler error to RetryManager", async () => {
    const kafka = createKafkaMock();
    const retryCalls = [];

    const retryManager = {
        async retry(message, error) {
            retryCalls.push({ message, error });
        }
    };

    const consumer = new KafkaConsumer(kafka, createConfig({
        handler: async () => {
            throw new Error("Processing failed");
        }
    }), retryManager);

    await consumer.connect();
    await consumer.start();

    await kafka.consumer.runCalls[0].eachMessage({
        topic: "order-create",
        partition: 1,
        message: createKafkaMessage()
    });

    assert.equal(retryCalls.length, 1);
    assert.equal(retryCalls[0].error.message, "Processing failed");
    assert.equal(retryCalls[0].message.data.orderId, "ORDER-1");
});

test("rethrows handler error when RetryManager is not configured", async () => {
    const kafka = createKafkaMock();

    const consumer = new KafkaConsumer(kafka, createConfig({
        handler: async () => {
            throw new Error("Processing failed");
        }
    }));

    await consumer.connect();
    await consumer.start();

    await assert.rejects(
        () => kafka.consumer.runCalls[0].eachMessage({
            topic: "order-create",
            partition: 1,
            message: createKafkaMessage()
        }),
        { message: "Processing failed" }
    );
});

test("disconnects consumer", async () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    await consumer.connect();
    await consumer.disconnect();

    assert.equal(kafka.consumer.disconnectCalls, 1);
    assert.equal(consumer.connected, false);
    assert.equal(consumer.running, false);
});

test("does not disconnect when already disconnected", async () => {
    const kafka = createKafkaMock();
    const consumer = new KafkaConsumer(kafka, createConfig());

    await consumer.disconnect();

    assert.equal(kafka.consumer.disconnectCalls, 0);
});