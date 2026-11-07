const test = require("node:test");
const assert = require("node:assert/strict");

const RetryManager = require("../../../src/retry/RetryManager");

function createRetryStore() {
    return {
        saved: [],
        scheduled: [],

        async save(retryId, data) {
            this.saved.push({ retryId, data });
        },

        async schedule(retryId, nextRetryAt) {
            this.scheduled.push({ retryId, nextRetryAt });
        }
    };
}

function createMessage(attempt = null) {
    return {
        data: { orderId: "ORDER-1" },
        meta: {
            messageId: "message-1",
            topic: "order-create",
            partition: 0,
            offset: "100",
            key: "ORDER-1",
            headers: {},
            consumer: "order-create",
            stage: attempt === null ? "normal" : "retry",
            original: {
                topic: "order-create",
                partition: 0,
                offset: "100"
            },
            retry: attempt === null ? null : {
                enabled: true,
                attempt,
                maxAttempts: 3,
                retryTopic: "order-create.retry",
                dlqTopic: "order-create.dlq"
            }
        }
    };
}

function createConfig(overrides = {}) {
    return {
        retry: {
            enabled: true,
            topic: "order-create.retry",
            strategy: "exponential",
            maxAttempts: 3,
            initialDelayMs: 1000,
            maxDelayMs: 10000,
            ...overrides.retry
        },
        dlq: {
            enabled: true,
            topic: "order-create.dlq",
            ...overrides.dlq
        }
    };
}

test("creates first retry attempt", async () => {
    const retryStore = createRetryStore();
    const manager = new RetryManager(createConfig(), retryStore);
    const result = await manager.retry(createMessage(), new Error("Processing failed"));

    assert.equal(result.action, "retry");
    assert.equal(result.attempt, 1);
    assert.equal(result.delayMs, 1000);
    assert.equal(retryStore.saved.length, 1);
    assert.equal(retryStore.scheduled.length, 1);
    assert.equal(retryStore.saved[0].data.attempt, 1);
    assert.equal(retryStore.saved[0].data.action, "retry");
});

test("creates second retry attempt", async () => {
    const retryStore = createRetryStore();
    const manager = new RetryManager(createConfig(), retryStore);
    const result = await manager.retry(createMessage(1), new Error("Processing failed"));

    assert.equal(result.action, "retry");
    assert.equal(result.attempt, 2);
    assert.equal(result.delayMs, 2000);
    assert.equal(retryStore.saved[0].data.attempt, 2);
});

test("creates third retry attempt", async () => {
    const retryStore = createRetryStore();
    const manager = new RetryManager(createConfig(), retryStore);
    const result = await manager.retry(createMessage(2), new Error("Processing failed"));

    assert.equal(result.action, "retry");
    assert.equal(result.attempt, 3);
    assert.equal(result.delayMs, 4000);
    assert.equal(retryStore.saved[0].data.attempt, 3);
});

test("sends message to DLQ after max attempts", async () => {
    const retryStore = createRetryStore();
    const manager = new RetryManager(createConfig(), retryStore);
    const result = await manager.retry(createMessage(3), new Error("Processing failed"));

    assert.equal(result.action, "dlq");
    assert.equal(result.reason, "max_attempts_exceeded");
    assert.equal(retryStore.saved.length, 1);
    assert.equal(retryStore.saved[0].data.action, "dlq");
    assert.equal(retryStore.saved[0].data.reason, "max_attempts_exceeded");
});

test("sends message to DLQ when retry is disabled", async () => {
    const retryStore = createRetryStore();
    const manager = new RetryManager(createConfig({ retry: { enabled: false } }), retryStore);
    const result = await manager.retry(createMessage(), new Error("Processing failed"));

    assert.equal(result.action, "dlq");
    assert.equal(result.reason, "retry_disabled");
    assert.equal(retryStore.saved[0].data.action, "dlq");
});

test("calculates fixed retry delay", () => {
    const retryStore = createRetryStore();
    const manager = new RetryManager(createConfig(), retryStore);

    assert.equal(manager.calculateDelay({
        strategy: "fixed",
        initialDelayMs: 2000,
        maxDelayMs: 10000,
        attempt: 1
    }), 2000);
});

test("caps fixed retry delay at max delay", () => {
    const retryStore = createRetryStore();
    const manager = new RetryManager(createConfig(), retryStore);

    assert.equal(manager.calculateDelay({
        strategy: "fixed",
        initialDelayMs: 15000,
        maxDelayMs: 10000,
        attempt: 1
    }), 10000);
});

test("calculates exponential retry delay", () => {
    const retryStore = createRetryStore();
    const manager = new RetryManager(createConfig(), retryStore);

    assert.equal(manager.calculateDelay({
        strategy: "exponential",
        initialDelayMs: 1000,
        maxDelayMs: 10000,
        attempt: 1
    }), 1000);

    assert.equal(manager.calculateDelay({
        strategy: "exponential",
        initialDelayMs: 1000,
        maxDelayMs: 10000,
        attempt: 2
    }), 2000);

    assert.equal(manager.calculateDelay({
        strategy: "exponential",
        initialDelayMs: 1000,
        maxDelayMs: 10000,
        attempt: 3
    }), 4000);
});

test("caps exponential retry delay at max delay", () => {
    const retryStore = createRetryStore();
    const manager = new RetryManager(createConfig(), retryStore);

    assert.equal(manager.calculateDelay({
        strategy: "exponential",
        initialDelayMs: 5000,
        maxDelayMs: 10000,
        attempt: 4
    }), 10000);
});

test("rejects unsupported retry strategy", () => {
    const retryStore = createRetryStore();
    const manager = new RetryManager(createConfig(), retryStore);

    assert.throws(() => manager.calculateDelay({
        strategy: "linear",
        initialDelayMs: 1000,
        maxDelayMs: 10000,
        attempt: 1
    }), { message: "Unsupported retry strategy: linear" });
});

test("requires retry manager configuration", () => {
    assert.throws(() => new RetryManager(null, createRetryStore()), { message: "Retry manager configuration is required" });
});

test("requires retry store", () => {
    assert.throws(() => new RetryManager(createConfig(), null), { message: "Retry store is required" });
});

test("requires message", async () => {
    const manager = new RetryManager(createConfig(), createRetryStore());

    await assert.rejects(() => manager.retry(null, new Error("failed")), { message: "Message is required" });
});

test("requires Error instance", async () => {
    const manager = new RetryManager(createConfig(), createRetryStore());

    await assert.rejects(() => manager.retry(createMessage(), "failed"), { message: "Retry error must be an Error" });
});

test("requires DLQ topic when retry is disabled", async () => {
    const retryStore = createRetryStore();
    const manager = new RetryManager({
        retry: { enabled: false },
        dlq: { enabled: false }
    }, retryStore);

    await assert.rejects(() => manager.retry(createMessage(), new Error("failed")), { message: "DLQ topic is required when message is sent to DLQ" });
});