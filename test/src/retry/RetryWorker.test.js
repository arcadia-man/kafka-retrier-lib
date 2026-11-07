const test = require("node:test");
const assert = require("node:assert/strict");

const RetryWorker = require("../../../src/retry/RetryWorker");

function createRetryStore(retryData = null) {
    return {
        recovered: [],
        dueRetries: [],
        claimed: [],
        completed: [],
        released: [],

        async recoverExpired(now, limit) {
            this.recovered.push({ now, limit });
            return [];
        },

        async getDueRetries(now, limit) {
            this.getDueRetriesArgs = { now, limit };
            return this.dueRetries;
        },

        async claim(retryId, leaseUntil) {
            this.claimed.push({ retryId, leaseUntil });
            return true;
        },

        async get() {
            return retryData;
        },

        async complete(retryId) {
            this.completed.push(retryId);
        },

        async release(retryId, nextRetryAt) {
            this.released.push({ retryId, nextRetryAt });
        }
    };
}

function createProducer() {
    return {
        sent: [],

        async send(data) {
            this.sent.push(data);
        }
    };
}

function createRetryData(overrides = {}) {
    return {
        retryId: "retry-1",
        action: "retry",
        message: {
            data: { orderId: "ORDER-1" },
            meta: {
                messageId: "message-1",
                topic: "order-create",
                partition: 1,
                offset: "100",
                key: "ORDER-1",
                headers: {},
                consumer: "order-create",
                stage: "normal",
                original: {
                    topic: "order-create",
                    partition: 1,
                    offset: "100"
                },
                retry: null
            }
        },
        error: {
            name: "Error",
            message: "Processing failed"
        },
        attempt: 1,
        maxAttempts: 3,
        retryTopic: "order-create.retry",
        dlqTopic: "order-create.dlq",
        ...overrides
    };
}

test("processes due retry and publishes retry message", async () => {
    const retryData = createRetryData();
    const retryStore = createRetryStore(retryData);
    retryStore.dueRetries = ["retry-1"];

    const producer = createProducer();
    const worker = new RetryWorker(retryStore, producer);

    await worker.process();

    assert.equal(retryStore.claimed.length, 1);
    assert.equal(producer.sent.length, 1);
    assert.equal(producer.sent[0].topic, "order-create.retry");
    assert.equal(producer.sent[0].messages[0].value, JSON.stringify({ orderId: "ORDER-1" }));
    assert.equal(retryStore.completed[0], "retry-1");
});

test("publishes retry metadata in Kafka headers", async () => {
    const retryData = createRetryData();
    const retryStore = createRetryStore(retryData);
    retryStore.dueRetries = ["retry-1"];

    const producer = createProducer();
    const worker = new RetryWorker(retryStore, producer);

    await worker.process();

    assert.deepEqual(producer.sent[0].messages[0].headers, {
        "message-id": "message-1",
        stage: "retry",
        "original-topic": "order-create",
        "original-partition": "1",
        "original-offset": "100",
        "retry-attempt": "1",
        "retry-max-attempts": "3",
        "retry-topic": "order-create.retry",
        "dlq-topic": "order-create.dlq"
    });
});

test("publishes DLQ message", async () => {
    const retryData = createRetryData({ action: "dlq", reason: "max_attempts_exceeded" });
    const retryStore = createRetryStore(retryData);
    retryStore.dueRetries = ["retry-1"];

    const producer = createProducer();
    const worker = new RetryWorker(retryStore, producer);

    await worker.process();

    assert.equal(producer.sent.length, 1);
    assert.equal(producer.sent[0].topic, "order-create.dlq");
    assert.equal(retryStore.completed[0], "retry-1");
});

test("publishes DLQ metadata in Kafka headers", async () => {
    const retryData = createRetryData({ action: "dlq", reason: "max_attempts_exceeded" });
    const retryStore = createRetryStore(retryData);
    retryStore.dueRetries = ["retry-1"];

    const producer = createProducer();
    const worker = new RetryWorker(retryStore, producer);

    await worker.process();

    assert.deepEqual(producer.sent[0].messages[0].headers, {
        "message-id": "message-1",
        stage: "dlq",
        "original-topic": "order-create",
        "original-partition": "1",
        "original-offset": "100",
        "retry-attempt": "1",
        "retry-max-attempts": "3",
        "retry-topic": "order-create.retry",
        "dlq-topic": "order-create.dlq"
    });
});

test("completes retry when retry data is missing", async () => {
    const retryStore = createRetryStore(null);
    retryStore.dueRetries = ["retry-1"];

    const producer = createProducer();
    const worker = new RetryWorker(retryStore, producer);

    await worker.process();

    assert.deepEqual(producer.sent, []);
    assert.deepEqual(retryStore.completed, ["retry-1"]);
});

test("releases retry when publishing fails", async () => {
    const retryData = createRetryData();
    const retryStore = createRetryStore(retryData);
    retryStore.dueRetries = ["retry-1"];

    const producer = {
        async send() {
            throw new Error("Kafka unavailable");
        }
    };

    const worker = new RetryWorker(retryStore, producer);

    await worker.process();

    assert.equal(retryStore.completed.length, 0);
    assert.equal(retryStore.released.length, 1);
    assert.equal(retryStore.released[0].retryId, "retry-1");
    assert.ok(retryStore.released[0].nextRetryAt >= Date.now());
});

test("does not process retry when claim fails", async () => {
    const retryData = createRetryData();
    const retryStore = createRetryStore(retryData);
    retryStore.dueRetries = ["retry-1"];
    retryStore.claim = async () => false;

    const producer = createProducer();
    const worker = new RetryWorker(retryStore, producer);

    await worker.process();

    assert.deepEqual(producer.sent, []);
    assert.deepEqual(retryStore.completed, []);
});

test("recovers expired retries before processing due retries", async () => {
    const retryStore = createRetryStore(createRetryData());
    retryStore.dueRetries = [];

    const producer = createProducer();
    const worker = new RetryWorker(retryStore, producer);

    await worker.process();

    assert.equal(retryStore.recovered.length, 1);
    assert.equal(retryStore.recovered[0].limit, 100);
});

test("uses configured batch size", async () => {
    const retryStore = createRetryStore();
    const producer = createProducer();
    const worker = new RetryWorker(retryStore, producer, { batchSize: 25 });

    await worker.process();

    assert.equal(retryStore.recovered[0].limit, 25);
    assert.equal(retryStore.getDueRetriesArgs.limit, 25);
});

test("requires retry store", () => {
    assert.throws(() => new RetryWorker(null, createProducer()), { message: "Retry store is required" });
});

test("requires Kafka producer", () => {
    assert.throws(() => new RetryWorker(createRetryStore(), null), { message: "Kafka producer is required" });
});

test("does not start worker twice", async () => {
    const retryStore = createRetryStore();
    const producer = createProducer();
    const worker = new RetryWorker(retryStore, producer, { pollIntervalMs: 100000 });

    await worker.start();
    await worker.start();

    assert.equal(worker.running, true);

    await worker.stop();
});

test("stops worker and clears timer", async () => {
    const retryStore = createRetryStore();
    const producer = createProducer();
    const worker = new RetryWorker(retryStore, producer, { pollIntervalMs: 100000 });

    await worker.start();
    await worker.stop();

    assert.equal(worker.running, false);
    assert.equal(worker.timer, null);
});

test("requires retry topic when publishing retry", async () => {
    const retryData = createRetryData({ retryTopic: null });
    const retryStore = createRetryStore(retryData);
    retryStore.dueRetries = ["retry-1"];

    const producer = createProducer();
    const worker = new RetryWorker(retryStore, producer);

    await worker.process();

    assert.equal(producer.sent.length, 0);
    assert.equal(retryStore.released.length, 1);
});

test("requires DLQ topic when publishing DLQ", async () => {
    const retryData = createRetryData({ action: "dlq", dlqTopic: null });
    const retryStore = createRetryStore(retryData);
    retryStore.dueRetries = ["retry-1"];

    const producer = createProducer();
    const worker = new RetryWorker(retryStore, producer);

    await worker.process();

    assert.equal(producer.sent.length, 0);
    assert.equal(retryStore.released.length, 1);
});

test("uses configured lease duration when claiming", async () => {
    const retryData = createRetryData();
    const retryStore = createRetryStore(retryData);
    retryStore.dueRetries = ["retry-1"];

    const producer = createProducer();
    const worker = new RetryWorker(retryStore, producer, { leaseDurationMs: 60000 });

    const before = Date.now();

    await worker.process();

    const after = Date.now();
    const leaseUntil = retryStore.claimed[0].leaseUntil;

    assert.ok(leaseUntil >= before + 60000);
    assert.ok(leaseUntil <= after + 60000);
});