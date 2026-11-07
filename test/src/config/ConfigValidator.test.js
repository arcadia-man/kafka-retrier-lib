const test = require("node:test");
const assert = require("node:assert/strict");

const ConfigValidator = require("../../../src/config/ConfigValidator");

function createValidConfig() {
    return {
        kafka: {
            clientId: "test-service",
            brokers: ["localhost:9092"]
        },
        redis: {
            url: "redis://localhost:6379"
        },
        consumers: [
            {
                name: "order-create",
                stage: "normal",
                groupId: "order-create-group",
                topic: "order-create",
                retry: {
                    enabled: true,
                    topic: "order-create.retry",
                    strategy: "exponential",
                    maxAttempts: 3,
                    initialDelayMs: 1000,
                    maxDelayMs: 10000
                },
                dlq: {
                    enabled: true,
                    topic: "order-create.dlq"
                },
                handler: async () => {}
            }
        ]
    };
}

test("accepts a valid complete configuration", () => {
    assert.equal(ConfigValidator.validate(createValidConfig()), true);
});

test("requires Kafka configuration", () => {
    const config = createValidConfig();
    delete config.kafka;

    assert.throws(() => ConfigValidator.validate(config), { message: "Kafka configuration is required" });
});

test("requires Kafka brokers", () => {
    const config = createValidConfig();
    config.kafka.brokers = [];

    assert.throws(() => ConfigValidator.validate(config), { message: "Kafka brokers are required" });
});

test("rejects an invalid Kafka broker", () => {
    const config = createValidConfig();
    config.kafka.brokers = ["localhost:9092", ""];

    assert.throws(() => ConfigValidator.validate(config), { message: "Kafka broker at index 1 must be a non-empty string" });
});

test("rejects an invalid Kafka clientId", () => {
    const config = createValidConfig();
    config.kafka.clientId = "";

    assert.throws(() => ConfigValidator.validate(config), { message: "Kafka clientId must be a non-empty string" });
});

test("requires Redis configuration", () => {
    const config = createValidConfig();
    delete config.redis;

    assert.throws(() => ConfigValidator.validate(config), { message: "Redis configuration is required" });
});

test("requires Redis URL", () => {
    const config = createValidConfig();
    config.redis.url = "";

    assert.throws(() => ConfigValidator.validate(config), { message: "Redis URL is required" });
});

test("accepts configuration without consumers", () => {
    const config = createValidConfig();
    delete config.consumers;

    assert.equal(ConfigValidator.validate(config), true);
});

test("requires consumers to be an array", () => {
    const config = createValidConfig();
    config.consumers = {};

    assert.throws(() => ConfigValidator.validate(config), { message: "Consumers must be an array" });
});

test("requires at least one consumer", () => {
    const config = createValidConfig();
    config.consumers = [];

    assert.throws(() => ConfigValidator.validate(config), { message: "At least one consumer must be configured" });
});

test("requires consumer name", () => {
    const config = createValidConfig();
    delete config.consumers[0].name;

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "0": name is required' });
});

test("requires consumer stage", () => {
    const config = createValidConfig();
    delete config.consumers[0].stage;

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": stage must be "normal", "retry", or "dlq"' });
});

test("rejects invalid consumer stage", () => {
    const config = createValidConfig();
    config.consumers[0].stage = "invalid";

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": stage must be "normal", "retry", or "dlq"' });
});

test("requires consumer groupId", () => {
    const config = createValidConfig();
    delete config.consumers[0].groupId;

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": groupId is required' });
});

test("requires consumer topic", () => {
    const config = createValidConfig();
    delete config.consumers[0].topic;

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": topic is required' });
});

test("requires consumer handler", () => {
    const config = createValidConfig();
    delete config.consumers[0].handler;

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": handler must be a function' });
});

test("rejects duplicate consumer names", () => {
    const config = createValidConfig();
    config.consumers.push({ ...config.consumers[0] });

    assert.throws(() => ConfigValidator.validate(config), { message: "Duplicate consumer name: order-create" });
});

test("accepts retry disabled", () => {
    const config = createValidConfig();
    config.consumers[0].retry = { enabled: false };

    assert.equal(ConfigValidator.validate(config), true);
});

test("requires retry topic when retry is enabled", () => {
    const config = createValidConfig();
    delete config.consumers[0].retry.topic;

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": retry.topic is required when retry is enabled' });
});

test("requires valid retry strategy", () => {
    const config = createValidConfig();
    config.consumers[0].retry.strategy = "linear";

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": retry.strategy must be "fixed" or "exponential"' });
});

test("requires positive retry maxAttempts", () => {
    const config = createValidConfig();
    config.consumers[0].retry.maxAttempts = 0;

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": retry.maxAttempts must be an integer greater than or equal to 1' });
});

test("requires positive retry initialDelayMs", () => {
    const config = createValidConfig();
    config.consumers[0].retry.initialDelayMs = 0;

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": retry.initialDelayMs must be an integer greater than or equal to 1' });
});

test("requires positive retry maxDelayMs", () => {
    const config = createValidConfig();
    config.consumers[0].retry.maxDelayMs = 0;

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": retry.maxDelayMs must be an integer greater than or equal to 1' });
});

test("rejects initial delay greater than max delay", () => {
    const config = createValidConfig();
    config.consumers[0].retry.initialDelayMs = 20000;

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": retry.initialDelayMs cannot be greater than retry.maxDelayMs' });
});

test("rejects retry and consumer topic conflict", () => {
    const config = createValidConfig();
    config.consumers[0].retry.topic = "order-create";

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": consumer topic and retry topic cannot be the same' });
});

test("rejects retry and DLQ topic conflict", () => {
    const config = createValidConfig();
    config.consumers[0].dlq.topic = "order-create.retry";

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": retry topic and dlq topic cannot be the same' });
});

test("rejects consumer and DLQ topic conflict", () => {
    const config = createValidConfig();
    config.consumers[0].dlq.topic = "order-create";

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": consumer topic and dlq topic cannot be the same' });
});

test("requires retry configuration for retry stage", () => {
    const config = createValidConfig();
    config.consumers[0].stage = "retry";
    config.consumers[0].retry = undefined;

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": retry configuration is required for retry stage' });
});

test("requires retry stage topic to match retry topic", () => {
    const config = createValidConfig();
    config.consumers[0].stage = "retry";
    config.consumers[0].topic = "wrong.retry";

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": retry stage consumer topic must match retry.topic' });
});

test("accepts a valid retry stage consumer", () => {
    const config = createValidConfig();
    config.consumers[0].stage = "retry";
    config.consumers[0].topic = "order-create.retry";

    assert.equal(ConfigValidator.validate(config), true);
});

test("rejects retry enabled on DLQ consumer", () => {
    const config = createValidConfig();
    config.consumers[0].stage = "dlq";
    config.consumers[0].topic = "order-create.dlq";

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": DLQ consumer cannot have retry enabled' });
});

test("accepts a valid DLQ consumer", () => {
    const config = createValidConfig();
    config.consumers[0].stage = "dlq";
    config.consumers[0].topic = "order-create.dlq";
    config.consumers[0].retry = { enabled: false };
    config.consumers[0].dlq = { enabled: false };

    assert.equal(ConfigValidator.validate(config), true);
});

test("accepts DLQ stage with enabled DLQ when topic matches", () => {
    const config = createValidConfig();
    config.consumers[0].stage = "dlq";
    config.consumers[0].topic = "order-create.dlq";
    config.consumers[0].retry = { enabled: false };

    assert.equal(ConfigValidator.validate(config), true);
});

test("rejects enabled DLQ stage when topic does not match DLQ topic", () => {
    const config = createValidConfig();
    config.consumers[0].stage = "dlq";
    config.consumers[0].topic = "wrong.dlq";
    config.consumers[0].retry = { enabled: false };

    assert.throws(() => ConfigValidator.validate(config), { message: 'Consumer "order-create": DLQ stage consumer topic must match dlq.topic' });
});