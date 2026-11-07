
// const test = require("node:test");
// const assert = require("node:assert/strict");

// const RetryStore = require("../../../src/retry/RetryStore");

// function createRedis() {
//     const redis = {
//         data: {},
//         zsets: {},

//         async set(key, value) {
//             this.data[key] = value;
//         },

//         async get(key) {
//             return this.data[key] ?? null;
//         },

//         async del(key) {
//             delete this.data[key];
//         },

//         async zAdd(key, item) {
//             if (!this.zsets[key]) {
//                 this.zsets[key] = [];
//             }

//             const existingIndex = this.zsets[key].findIndex(
//                 (entry) => entry.value === item.value
//             );

//             if (existingIndex !== -1) {
//                 this.zsets[key][existingIndex] = {
//                     value: item.value,
//                     score: Number(item.score),
//                 };
//             } else {
//                 this.zsets[key].push({
//                     value: item.value,
//                     score: Number(item.score),
//                 });
//             }
//         },

//         async zRem(key, value) {
//             if (!this.zsets[key]) {
//                 return 0;
//             }

//             const before = this.zsets[key].length;

//             this.zsets[key] = this.zsets[key].filter(
//                 (entry) => entry.value !== value
//             );

//             return before - this.zsets[key].length;
//         },

//         async zRangeByScore(key, min, max, options = {}) {
//             const entries = (this.zsets[key] ?? [])
//                 .filter(
//                     (entry) =>
//                         entry.score >= Number(min) &&
//                         entry.score <= Number(max)
//                 )
//                 .sort((a, b) => a.score - b.score);

//             const offset = options.LIMIT?.offset ?? 0;
//             const count = options.LIMIT?.count ?? entries.length;

//             return entries
//                 .slice(offset, offset + count)
//                 .map((entry) => entry.value);
//         },

//         async eval(script, { keys, arguments: args }) {
//             const [scheduledKey, processingKey] = keys;
//             const [retryId, leaseUntil] = args;

//             const scheduled = this.zsets[scheduledKey] ?? [];

//             const index = scheduled.findIndex(
//                 (entry) => entry.value === retryId
//             );

//             if (index === -1) {
//                 return 0;
//             }

//             scheduled.splice(index, 1);

//             if (!this.zsets[processingKey]) {
//                 this.zsets[processingKey] = [];
//             }

//             this.zsets[processingKey].push({
//                 value: retryId,
//                 score: Number(leaseUntil),
//             });

//             this.zsets[scheduledKey] = scheduled;

//             return 1;
//         },
//     };

//     return redis;
// }

// function createRetryData() {
//     return {
//         retryId: "retry-1",

//         message: {
//             data: {
//                 orderId: "ORD-1001",
//             },

//             meta: {
//                 messageId: "message-1",
//                 topic: "order-create",
//                 partition: 2,
//                 offset: "100",
//             },
//         },

//         attempt: 1,
//         maxAttempts: 3,

//         retryTopic: "order-create.retry",
//         dlqTopic: "order-create.dlq",

//         createdAt: Date.now(),
//         nextRetryAt: Date.now() + 1000,
//     };
// }

// test("requires Redis client", () => {
//     assert.throws(
//         () => new RetryStore(),
//         /Redis client is required/
//     );
// });

// test("saves retry data", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     const data = createRetryData();

//     await store.save("retry-1", data);

//     assert.equal(
//         redis.data["retry:data:retry-1"],
//         JSON.stringify(data)
//     );
// });

// test("requires retry ID when saving", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await assert.rejects(
//         () => store.save("", createRetryData()),
//         /Retry ID is required/
//     );
// });

// test("requires retry data when saving", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await assert.rejects(
//         () => store.save("retry-1", null),
//         /Retry data is required/
//     );
// });

// test("gets saved retry data", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     const data = createRetryData();

//     await store.save("retry-1", data);

//     const result = await store.get("retry-1");

//     assert.deepEqual(result, data);
// });

// test("returns null when retry data does not exist", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     const result = await store.get("missing-retry");

//     assert.equal(result, null);
// });

// test("requires retry ID when getting data", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await assert.rejects(
//         () => store.get(""),
//         /Retry ID is required/
//     );
// });

// test("schedules retry", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     const nextRetryAt = Date.now() + 5000;

//     await store.schedule(
//         "retry-1",
//         nextRetryAt
//     );

//     assert.deepEqual(
//         redis.zsets["retry:scheduled"],
//         [
//             {
//                 value: "retry-1",
//                 score: nextRetryAt,
//             },
//         ]
//     );
// });

// test("requires retry ID when scheduling", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await assert.rejects(
//         () => store.schedule("", Date.now()),
//         /Retry ID is required/
//     );
// });

// test("requires valid timestamp when scheduling", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await assert.rejects(
//         () => store.schedule("retry-1", NaN),
//         /Next retry time must be a valid timestamp/
//     );
// });

// test("returns due retries", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     const now = Date.now();

//     await store.schedule(
//         "retry-past",
//         now - 1000
//     );

//     await store.schedule(
//         "retry-now",
//         now
//     );

//     await store.schedule(
//         "retry-future",
//         now + 10000
//     );

//     const result =
//         await store.getDueRetries(now);

//     assert.deepEqual(
//         result,
//         [
//             "retry-past",
//             "retry-now",
//         ]
//     );
// });

// test("does not return future retries", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     const now = Date.now();

//     await store.schedule(
//         "retry-future",
//         now + 10000
//     );

//     const result =
//         await store.getDueRetries(now);

//     assert.deepEqual(result, []);
// });

// test("returns due retries in scheduled order", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     const now = Date.now();

//     await store.schedule(
//         "retry-3",
//         now - 100
//     );

//     await store.schedule(
//         "retry-1",
//         now - 300
//     );

//     await store.schedule(
//         "retry-2",
//         now - 200
//     );

//     const result =
//         await store.getDueRetries(now);

//     assert.deepEqual(
//         result,
//         [
//             "retry-1",
//             "retry-2",
//             "retry-3",
//         ]
//     );
// });

// test("respects due retry limit", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     const now = Date.now();

//     await store.schedule("retry-1", now - 300);
//     await store.schedule("retry-2", now - 200);
//     await store.schedule("retry-3", now - 100);

//     const result =
//         await store.getDueRetries(now, 2);

//     assert.deepEqual(
//         result,
//         [
//             "retry-1",
//             "retry-2",
//         ]
//     );
// });

// test("requires valid current time when getting due retries", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await assert.rejects(
//         () => store.getDueRetries(NaN),
//         /Current time must be a valid timestamp/
//     );
// });

// test("requires positive integer limit", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await assert.rejects(
//         () => store.getDueRetries(Date.now(), 0),
//         /Limit must be a positive integer/
//     );

//     await assert.rejects(
//         () => store.getDueRetries(Date.now(), 1.5),
//         /Limit must be a positive integer/
//     );
// });

// test("claims scheduled retry", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     const nextRetryAt = Date.now() - 1000;

//     await store.schedule(
//         "retry-1",
//         nextRetryAt
//     );

//     const leaseUntil =
//         Date.now() + 30000;

//     const result =
//         await store.claim(
//             "retry-1",
//             leaseUntil
//         );

//     assert.equal(result, true);

//     assert.deepEqual(
//         redis.zsets["retry:scheduled"],
//         []
//     );

//     assert.deepEqual(
//         redis.zsets["retry:processing"],
//         [
//             {
//                 value: "retry-1",
//                 score: leaseUntil,
//             },
//         ]
//     );
// });

// test("cannot claim the same retry twice", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await store.schedule(
//         "retry-1",
//         Date.now() - 1000
//     );

//     const leaseUntil =
//         Date.now() + 30000;

//     const firstClaim =
//         await store.claim(
//             "retry-1",
//             leaseUntil
//         );

//     const secondClaim =
//         await store.claim(
//             "retry-1",
//             leaseUntil
//         );

//     assert.equal(firstClaim, true);
//     assert.equal(secondClaim, false);
// });

// test("cannot claim retry that was never scheduled", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     const result =
//         await store.claim(
//             "missing-retry",
//             Date.now() + 30000
//         );

//     assert.equal(result, false);
// });

// test("requires retry ID when claiming", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await assert.rejects(
//         () =>
//             store.claim(
//                 "",
//                 Date.now() + 30000
//             ),
//         /Retry ID is required/
//     );
// });

// test("requires valid lease timestamp when claiming", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await assert.rejects(
//         () =>
//             store.claim(
//                 "retry-1",
//                 NaN
//             ),
//         /Lease expiry must be a valid timestamp/
//     );
// });

// test("complete removes processing state and retry data", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     const data = createRetryData();

//     await store.save(
//         "retry-1",
//         data
//     );

//     await store.schedule(
//         "retry-1",
//         Date.now() - 1000
//     );

//     await store.claim(
//         "retry-1",
//         Date.now() + 30000
//     );

//     await store.complete("retry-1");

//     assert.deepEqual(
//         redis.zsets["retry:processing"],
//         []
//     );

//     assert.equal(
//         redis.data["retry:data:retry-1"],
//         undefined
//     );
// });

// test("requires retry ID when completing", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await assert.rejects(
//         () => store.complete(""),
//         /Retry ID is required/
//     );
// });

// test("release moves retry back to scheduled state", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await store.schedule(
//         "retry-1",
//         Date.now() - 1000
//     );

//     await store.claim(
//         "retry-1",
//         Date.now() + 30000
//     );

//     const nextRetryAt =
//         Date.now() + 5000;

//     await store.release(
//         "retry-1",
//         nextRetryAt
//     );

//     assert.deepEqual(
//         redis.zsets["retry:processing"],
//         []
//     );

//     assert.deepEqual(
//         redis.zsets["retry:scheduled"],
//         [
//             {
//                 value: "retry-1",
//                 score: nextRetryAt,
//             },
//         ]
//     );
// });

// test("requires retry ID when releasing", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await assert.rejects(
//         () =>
//             store.release(
//                 "",
//                 Date.now()
//             ),
//         /Retry ID is required/
//     );
// });

// test("requires valid timestamp when releasing", async () => {
//     const redis = createRedis();
//     const store = new RetryStore(redis);

//     await assert.rejects(
//         () =>
//             store.release(
//                 "retry-1",
//                 NaN
//             ),
//         /Next retry time must be a valid timestamp/
//     );
// });

// test("exposes Redis key names", () => {
//     assert.deepEqual(
//         RetryStore.keys,
//         {
//             RETRY_DATA_PREFIX: "retry:data:",
//             RETRY_SCHEDULED_KEY: "retry:scheduled",
//             RETRY_PROCESSING_KEY: "retry:processing",
//         }
//     );
// });
