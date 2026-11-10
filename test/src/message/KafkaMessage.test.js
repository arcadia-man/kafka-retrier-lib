const test = require("node:test");
const assert = require("node:assert/strict");

const KafkaMessage = require("../../../src/message/KafkaMessage");

test("creates message with data and metadata", () => {
    const message = new KafkaMessage({
        data: { orderId: "ORDER-1", amount: 100 },
        messageId: "message-1",
        topic: "order-create",
        partition: 2,
        offset: "100",
        key: "ORDER-1",
        headers: { source: "test" },
        consumer: "order-create",
        stage: "normal",
        original: { topic: "order-create", partition: 2, offset: "100" },
        retry: null
    });

    assert.deepEqual(message.data, { orderId: "ORDER-1", amount: 100 });
    assert.deepEqual(message.meta, {
        messageId: "message-1",
        topic: "order-create",
        partition: 2,
        offset: "100",
        key: "ORDER-1",
        headers: { source: "test" },
        consumer: "order-create",
        stage: "normal",
        original: { topic: "order-create", partition: 2, offset: "100" },
        retry: null
    });
});

test("preserves retry metadata", () => {
    const retry = {
        enabled: true,
        attempt: 2,
        maxAttempts: 3,
        retryTopic: "order-create.retry",
        dlqTopic: "order-create.dlq"
    };

    const message = new KafkaMessage({
        data: { orderId: "FAIL" },
        messageId: "message-2",
        topic: "order-create.retry",
        partition: 1,
        offset: "20",
        key: "FAIL",
        headers: {},
        consumer: "order-create-retry",
        stage: "retry",
        original: { topic: "order-create", partition: 0, offset: "10" },
        retry
    });

    assert.deepEqual(message.meta.retry, retry);
});

test("preserves null values", () => {
    const message = new KafkaMessage({
        data: null,
        messageId: null,
        topic: "test",
        partition: 0,
        offset: "0",
        key: null,
        headers: {},
        consumer: "test",
        stage: "normal",
        original: { topic: "test", partition: 0, offset: "0" },
        retry: null
    });

    assert.equal(message.data, null);
    assert.equal(message.meta.messageId, null);
    assert.equal(message.meta.key, null);
    assert.equal(message.meta.retry, null);
});

test("keeps metadata isolated between message instances", () => {
    const message1 = new KafkaMessage({
        data: { id: 1 },
        messageId: "1",
        topic: "test",
        partition: 0,
        offset: "1",
        key: "1",
        headers: {},
        consumer: "consumer-1",
        stage: "normal",
        original: { topic: "test", partition: 0, offset: "1" },
        retry: null
    });

    const message2 = new KafkaMessage({
        data: { id: 2 },
        messageId: "2",
        topic: "test",
        partition: 1,
        offset: "2",
        key: "2",
        headers: {},
        consumer: "consumer-2",
        stage: "retry",
        original: { topic: "test", partition: 1, offset: "2" },
        retry: { attempt: 1 }
    });

    message1.meta.headers.source = "message-1";

    assert.deepEqual(message1.meta.headers, { source: "message-1" });
    assert.deepEqual(message2.meta.headers, {});
});

test("provides direct property access via getters", () => {
    const message = new KafkaMessage({
        data: { id: 123 },
        messageId: "msg-123",
        topic: "orders",
        partition: 2,
        offset: "456",
        key: "order-123",
        headers: { "x-trace-id": "trace-1" },
        consumer: "order-consumer",
        stage: "retry",
        original: { topic: "orders", partition: 2, offset: "100" },
        retry: { attempt: 2 }
    });

    assert.equal(message.messageId, "msg-123");
    assert.equal(message.topic, "orders");
    assert.equal(message.partition, 2);
    assert.equal(message.offset, "456");
    assert.equal(message.key, "order-123");
    assert.deepEqual(message.headers, { "x-trace-id": "trace-1" });
    assert.equal(message.consumer, "order-consumer");
    assert.equal(message.stage, "retry");
    assert.deepEqual(message.original, { topic: "orders", partition: 2, offset: "100" });
    assert.deepEqual(message.retry, { attempt: 2 });
});