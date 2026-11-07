class ConfigValidator {
    static validate(config = {}) {
        this.validateKafka(config.kafka);
        this.validateRedis(config.redis);

        if (config.consumers !== undefined) this.validateConsumers(config.consumers);

        return true;
    }

    static validateKafka(kafka) {
        if (!kafka || typeof kafka !== "object") throw new Error("Kafka configuration is required");
        if (!Array.isArray(kafka.brokers) || kafka.brokers.length === 0) throw new Error("Kafka brokers are required");

        kafka.brokers.forEach((broker, index) => {
            if (typeof broker !== "string" || broker.trim() === "") throw new Error(`Kafka broker at index ${index} must be a non-empty string`);
        });

        if (kafka.clientId !== undefined && (typeof kafka.clientId !== "string" || kafka.clientId.trim() === "")) throw new Error("Kafka clientId must be a non-empty string");
    }

    static validateRedis(redis) {
        if (!redis || typeof redis !== "object") throw new Error("Redis configuration is required");
        if (typeof redis.url !== "string" || redis.url.trim() === "") throw new Error("Redis URL is required");
    }

    static validateConsumers(consumers) {
        if (!Array.isArray(consumers)) throw new Error("Consumers must be an array");
        if (consumers.length === 0) throw new Error("At least one consumer must be configured");

        const consumerNames = new Set();

        consumers.forEach((consumer, index) => {
            this.validateConsumer(consumer, index);

            if (consumerNames.has(consumer.name)) throw new Error(`Duplicate consumer name: ${consumer.name}`);

            consumerNames.add(consumer.name);
        });
    }

    static validateConsumer(consumer, index) {
        if (!consumer || typeof consumer !== "object") throw new Error(`Consumer at index ${index} must be an object`);

        const prefix = `Consumer "${consumer.name ?? index}"`;

        if (typeof consumer.name !== "string" || consumer.name.trim() === "") throw new Error(`${prefix}: name is required`);
        if (typeof consumer.stage !== "string" || !["normal", "retry", "dlq"].includes(consumer.stage)) throw new Error(`${prefix}: stage must be "normal", "retry", or "dlq"`);
        if (typeof consumer.groupId !== "string" || consumer.groupId.trim() === "") throw new Error(`${prefix}: groupId is required`);
        if (typeof consumer.topic !== "string" || consumer.topic.trim() === "") throw new Error(`${prefix}: topic is required`);
        if (typeof consumer.handler !== "function") throw new Error(`${prefix}: handler must be a function`);

        this.validateRetry(consumer.retry, prefix);
        this.validateDlq(consumer.dlq, prefix);
        this.validateStageConfig(consumer, prefix);
        this.validateTopicConflicts(consumer, prefix);
    }

    static validateStageConfig(consumer, prefix) {
        if (consumer.stage === "normal") return;

        if (consumer.stage === "retry") {
            if (!consumer.retry || consumer.retry.enabled === false) throw new Error(`${prefix}: retry configuration is required for retry stage`);
            if (consumer.topic !== consumer.retry.topic) throw new Error(`${prefix}: retry stage consumer topic must match retry.topic`);
            return;
        }

        if (consumer.stage === "dlq") {
            if (consumer.retry?.enabled === true) throw new Error(`${prefix}: DLQ consumer cannot have retry enabled`);
            if (consumer.dlq?.enabled === true && consumer.topic !== consumer.dlq.topic) throw new Error(`${prefix}: DLQ stage consumer topic must match dlq.topic`);
        }
    }

    static validateRetry(retry, prefix) {
        if (retry === undefined || retry === null) return;
        if (typeof retry !== "object") throw new Error(`${prefix}: retry must be an object`);
        if (retry.enabled !== undefined && typeof retry.enabled !== "boolean") throw new Error(`${prefix}: retry.enabled must be boolean`);
        if (retry.enabled === false) return;

        if (typeof retry.topic !== "string" || retry.topic.trim() === "") throw new Error(`${prefix}: retry.topic is required when retry is enabled`);

        if (!["fixed", "exponential"].includes(retry.strategy)) throw new Error(`${prefix}: retry.strategy must be "fixed" or "exponential"`);

        this.validatePositiveInteger(retry.maxAttempts, `${prefix}: retry.maxAttempts`, { min: 1 });
        this.validatePositiveInteger(retry.initialDelayMs, `${prefix}: retry.initialDelayMs`, { min: 1 });
        this.validatePositiveInteger(retry.maxDelayMs, `${prefix}: retry.maxDelayMs`, { min: 1 });

        if (retry.initialDelayMs > retry.maxDelayMs) throw new Error(`${prefix}: retry.initialDelayMs cannot be greater than retry.maxDelayMs`);
    }

    static validateDlq(dlq, prefix) {
        if (dlq === undefined || dlq === null) return;
        if (typeof dlq !== "object") throw new Error(`${prefix}: dlq must be an object`);
        if (dlq.enabled !== undefined && typeof dlq.enabled !== "boolean") throw new Error(`${prefix}: dlq.enabled must be boolean`);
        if (dlq.enabled === false) return;

        if (typeof dlq.topic !== "string" || dlq.topic.trim() === "") throw new Error(`${prefix}: dlq.topic is required when DLQ is enabled`);
    }

    static validateTopicConflicts(consumer, prefix) {
        const topics = new Map();

        topics.set("consumer", consumer.topic);

        if (consumer.stage !== "retry" && consumer.retry?.enabled !== false && consumer.retry?.topic) topics.set("retry", consumer.retry.topic);
        if (consumer.stage !== "dlq" && consumer.dlq?.enabled !== false && consumer.dlq?.topic) topics.set("dlq", consumer.dlq.topic);

        const entries = [...topics.entries()];

        for (let i = 0; i < entries.length; i++) {
            for (let j = i + 1; j < entries.length; j++) {
                if (entries[i][1] === entries[j][1]) throw new Error(`${prefix}: ${entries[i][0]} topic and ${entries[j][0]} topic cannot be the same`);
            }
        }
    }

    static validatePositiveInteger(value, field, options = {}) {
        if (value === undefined) return;

        const min = options.min ?? 1;

        if (!Number.isInteger(value) || value < min) throw new Error(`${field} must be an integer greater than or equal to ${min}`);
    }
}

module.exports = ConfigValidator;