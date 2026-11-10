const crypto = require("node:crypto");

class RetryManager {
    constructor(config, retryStore) {
        if (!config || typeof config !== "object") throw new Error("Retry manager configuration is required");
        if (!retryStore) throw new Error("Retry store is required");

        this.config = config;
        this.retryStore = retryStore;
    }

    async retry(message, error) {
        if (!message) throw new Error("Message is required");
        if (!(error instanceof Error)) throw new Error("Retry error must be an Error");

        const retryConfig = this.config.retry;

        if (!retryConfig || retryConfig.enabled === false) return this.createDlqJob(message, error);

        const currentAttempt = message.meta.retry?.attempt ?? 0;
        const nextAttempt = currentAttempt + 1;

        if (nextAttempt > retryConfig.maxAttempts) return this.createDlqJob(message, error, "max_attempts_exceeded");

        const initialDelayMs = retryConfig.initialDelayMs ?? retryConfig.delayMs;
        const maxDelayMs = retryConfig.maxDelayMs ?? retryConfig.delayMs;

        const delayMs = this.calculateDelay({
            strategy: retryConfig.strategy,
            initialDelayMs,
            maxDelayMs,
            attempt: nextAttempt
        });

        const retryId = crypto.randomUUID();
        const now = Date.now();
        const nextRetryAt = now + delayMs;

        const retryData = {
            retryId,
            action: "retry",
            message,
            error: {
                name: error.name,
                message: error.message
            },
            attempt: nextAttempt,
            maxAttempts: retryConfig.maxAttempts,
            retryTopic: retryConfig.topic,
            dlqTopic: this.config.dlq?.topic ?? null,
            strategy: retryConfig.strategy,
            initialDelayMs,
            maxDelayMs,
            createdAt: now,
            nextRetryAt
        };

        const ttlSeconds = Math.max(86400, Math.ceil((delayMs + 60000) / 1000) * 2);
        await this.retryStore.save(retryId, retryData, ttlSeconds);

        try {
            await this.retryStore.schedule(retryId, nextRetryAt);
        } catch (scheduleError) {
            if (typeof this.retryStore.complete === "function") {
                await this.retryStore.complete(retryId).catch(() => {});
            }
            throw scheduleError;
        }

        return {
            action: "retry",
            retryId,
            attempt: nextAttempt,
            delayMs,
            nextRetryAt
        };
    }

    async createDlqJob(message, error, reason = "retry_disabled") {
        const dlqTopic = this.config.dlq?.topic;

        if (!dlqTopic) throw new Error("DLQ topic is required when message is sent to DLQ");

        const retryId = crypto.randomUUID();
        const now = Date.now();

        const retryData = {
            retryId,
            action: "dlq",
            message,
            error: {
                name: error.name,
                message: error.message
            },
            attempt: message.meta.retry?.attempt ?? 0,
            maxAttempts: this.config.retry?.maxAttempts ?? 0,
            retryTopic: this.config.retry?.topic ?? null,
            dlqTopic,
            reason,
            createdAt: now,
            nextRetryAt: now
        };

        const ttlSeconds = 86400;
        await this.retryStore.save(retryId, retryData, ttlSeconds);

        try {
            await this.retryStore.schedule(retryId, now);
        } catch (scheduleError) {
            if (typeof this.retryStore.complete === "function") {
                await this.retryStore.complete(retryId).catch(() => {});
            }
            throw scheduleError;
        }

        return {
            action: "dlq",
            retryId,
            reason
        };
    }

    calculateDelay({ strategy, initialDelayMs, maxDelayMs, attempt }) {
        if (strategy === "fixed") {
            const delay = initialDelayMs ?? maxDelayMs;
            if (delay === undefined) throw new Error("Fixed retry strategy requires delayMs or initialDelayMs");
            return Math.min(initialDelayMs ?? delay, maxDelayMs ?? delay);
        }

        if (strategy === "exponential") {
            if (initialDelayMs === undefined || maxDelayMs === undefined) {
                throw new Error("Exponential retry strategy requires initialDelayMs and maxDelayMs");
            }
            const delay = initialDelayMs * 2 ** (attempt - 1);
            return Math.min(delay, maxDelayMs);
        }

        throw new Error(`Unsupported retry strategy: ${strategy}`);
    }
}

module.exports = RetryManager;