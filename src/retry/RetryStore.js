const test = require("node:test");
const assert = require("node:assert/strict");

const RetryStore = require("../../../src/retry/RetryStore");

function createRedisMock() {
    const data = new Map();
    const sortedSets = new Map();

    return {
        data,
        sortedSets,

        async set(key, value) {
            data.set(key, value);
        },

        async get(key) {
            return data.get(key) ?? null;
        },

        async del(key) {
            data.delete(key);
        },

        async zAdd(key, { score, value }) {
            if (!sortedSets.has(key)) sortedSets.set(key, new Map());
            sortedSets.get(key).set(value, Number(score));
        },

        async zRem(key, value) {
            sortedSets.get(key)?.delete(value);
        },

        async zRangeByScore(key, min, max, options = {}) {
            const entries = [...(sortedSets.get(key) ?? new Map()).entries()]
                .filter(([, score]) => score >= Number(min) && score <= Number(max))
                .sort((a, b) => a[1] - b[1]);

            const offset = options.LIMIT?.offset ?? 0;
            const count = options.LIMIT?.count ?? entries.length;

            return entries.slice(offset, offset + count).map(([value]) => value);
        },

        async eval(script, { keys, arguments: args }) {
            if (script.includes("ZREM") && script.includes("ZADD")) {
                const [scheduledKey, processingKey] = keys;
                const [retryId, leaseUntil] = args;

                const scheduled = sortedSets.get(scheduledKey);

                if (!scheduled?.has(retryId)) return 0;

                scheduled.delete(retryId);

                if (!sortedSets.has(processingKey)) sortedSets.set(processingKey, new Map());

                sortedSets.get(processingKey).set(retryId, Number(leaseUntil));

                return 1;
            }

            if (script.includes("ZRANGEBYSCORE")) {
                const [processingKey, scheduledKey] = keys;
                const [now, limit] = args;
                const processing = sortedSets.get(processingKey) ?? new Map();
                const expired = [...processing.entries()]
                    .filter(([, score]) => score <= Number(now))
                    .sort((a, b) => a[1] - b[1])
                    .slice(0, Number(limit))
                    .map(([retryId]) => retryId);

                for (const retryId of expired) {
                    processing.delete(retryId);

                    if (!sortedSets.has(scheduledKey)) sortedSets.set(scheduledKey, new Map());

                    sortedSets.get(scheduledKey).set(retryId, Number(now));
                }

                return expired;
            }

            throw new Error("Unsupported Redis script");
        }
    };
}

function createStore() {
    return {
        redis: createRedisMock()
    };
}

test("saves and gets retry data", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);
    const retryData = { retryId: "retry-1", action: "retry", attempt: 1 };

    await store.save("retry-1", retryData);

    assert.deepEqual(await store.get("retry-1"), retryData);
});

test("returns null when retry data does not exist", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    assert.equal(await store.get("missing"), null);
});

test("schedules retry", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    await store.schedule("retry-1", 5000);

    assert.deepEqual(redis.sortedSets.get(RetryStore.keys.RETRY_SCHEDULED_KEY), new Map([["retry-1", 5000]]));
});

test("gets due retries", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    await store.schedule("retry-1", 1000);
    await store.schedule("retry-2", 2000);
    await store.schedule("retry-3", 5000);

    assert.deepEqual(await store.getDueRetries(2000, 10), ["retry-1", "retry-2"]);
});

test("respects due retry limit", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    await store.schedule("retry-1", 1000);
    await store.schedule("retry-2", 2000);
    await store.schedule("retry-3", 3000);

    assert.deepEqual(await store.getDueRetries(5000, 2), ["retry-1", "retry-2"]);
});

test("claims scheduled retry", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    await store.schedule("retry-1", 1000);

    const claimed = await store.claim("retry-1", 10000);

    assert.equal(claimed, true);
    assert.equal(redis.sortedSets.get(RetryStore.keys.RETRY_SCHEDULED_KEY)?.has("retry-1"), false);
    assert.equal(redis.sortedSets.get(RetryStore.keys.RETRY_PROCESSING_KEY)?.get("retry-1"), 10000);
});

test("cannot claim retry that is not scheduled", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    assert.equal(await store.claim("missing", 10000), false);
});

test("completes retry and removes all retry state", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    await store.save("retry-1", { retryId: "retry-1" });
    await store.schedule("retry-1", 1000);
    await store.claim("retry-1", 10000);

    await store.complete("retry-1");

    assert.equal(await store.get("retry-1"), null);
    assert.equal(redis.sortedSets.get(RetryStore.keys.RETRY_SCHEDULED_KEY)?.has("retry-1"), false);
    assert.equal(redis.sortedSets.get(RetryStore.keys.RETRY_PROCESSING_KEY)?.has("retry-1"), false);
});

test("releases processing retry back to scheduled state", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    await store.schedule("retry-1", 1000);
    await store.claim("retry-1", 10000);

    await store.release("retry-1", 20000);

    assert.equal(redis.sortedSets.get(RetryStore.keys.RETRY_PROCESSING_KEY)?.has("retry-1"), false);
    assert.equal(redis.sortedSets.get(RetryStore.keys.RETRY_SCHEDULED_KEY)?.get("retry-1"), 20000);
});

test("recovers expired processing retries", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    await store.schedule("retry-1", 1000);
    await store.schedule("retry-2", 1000);

    await store.claim("retry-1", 5000);
    await store.claim("retry-2", 15000);

    const recovered = await store.recoverExpired(10000, 100);

    assert.deepEqual(recovered, ["retry-1"]);
    assert.equal(redis.sortedSets.get(RetryStore.keys.RETRY_PROCESSING_KEY)?.has("retry-1"), false);
    assert.equal(redis.sortedSets.get(RetryStore.keys.RETRY_SCHEDULED_KEY)?.get("retry-1"), 10000);
    assert.equal(redis.sortedSets.get(RetryStore.keys.RETRY_PROCESSING_KEY)?.get("retry-2"), 15000);
});

test("requires Redis client", () => {
    assert.throws(() => new RetryStore(), { message: "Redis client is required" });
});

test("requires retry ID when saving", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    await assert.rejects(() => store.save("", {}), { message: "Retry ID is required" });
});

test("requires retry data when saving", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    await assert.rejects(() => store.save("retry-1", null), { message: "Retry data is required" });
});

test("requires valid retry timestamp when scheduling", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    await assert.rejects(() => store.schedule("retry-1", NaN), { message: "Next retry time must be a valid timestamp" });
});

test("requires positive limit for due retries", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    await assert.rejects(() => store.getDueRetries(Date.now(), 0), { message: "Limit must be a positive integer" });
});

test("requires valid timestamp for recovery", async () => {
    const { redis } = createStore();
    const store = new RetryStore(redis);

    await assert.rejects(() => store.recoverExpired(NaN, 10), { message: "Current time must be a valid timestamp" });
});