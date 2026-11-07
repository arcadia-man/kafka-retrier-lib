class RetryWorker {
    constructor(retryStore, kafkaProducer, config = {}) {
        if (!retryStore) throw new Error("Retry store is required");
        if (!kafkaProducer) throw new Error("Kafka producer is required");

        this.retryStore = retryStore;
        this.kafkaProducer = kafkaProducer;
        this.batchSize = config.batchSize ?? 100;
        this.pollIntervalMs = config.pollIntervalMs ?? 1000;
        this.leaseDurationMs = config.leaseDurationMs ?? 30000;
        this.running = false;
        this.timer = null;
    }

    async start() {
        if (this.running) return;
        this.running = true;
        await this.process();
        this.scheduleNextPoll();
    }

    async stop() {
        this.running = false;
        if (this.timer) clearTimeout(this.timer);
        this.timer = null;
    }

    scheduleNextPoll() {
        if (!this.running) return;
        const delay = Math.max(0, this.pollIntervalMs);

        this.timer = setTimeout(async () => {
            try {
                await this.process();
            } catch (error) {
                console.error("Retry worker error:", error);
            }

            this.scheduleNextPoll();
        }, delay);
    }

    async process() {
        const now = Date.now();

        await this.retryStore.recoverExpired(now, this.batchSize);

        const retryIds = await this.retryStore.getDueRetries(now, this.batchSize);
        for (const retryId of retryIds) await this.processRetry(retryId);
    }

    async processRetry(retryId) {
        const leaseUntil = Date.now() + this.leaseDurationMs;
        const claimed = await this.retryStore.claim(retryId, leaseUntil);

        if (!claimed) return;

        const retryData = await this.retryStore.get(retryId);

        if (!retryData) {
            await this.retryStore.complete(retryId);
            return;
        }

        try {
            if (retryData.action === "dlq") {
                await this.publishDlq(retryData);
            } else {
                await this.publishRetry(retryData);
            }

            await this.retryStore.complete(retryId);
        } catch (error) {
            console.error(`Retry worker publish failed for ${retryId}:`, error);
            await this.retryStore.release(retryId, Date.now() + this.calculateFailureDelay());
        }
    }

    async publishRetry(retryData) {
        if (!retryData.retryTopic) throw new Error("Retry topic is required");

        const message = retryData.message;

        await this.kafkaProducer.send({
            topic: retryData.retryTopic,
            messages: [{
                key: message.meta?.key ?? null,
                value: JSON.stringify(message.data),
                headers: {
                    "message-id": message.meta?.messageId ?? "",
                    stage: "retry",
                    "original-topic": message.meta?.original?.topic ?? message.meta?.topic ?? "",
                    "original-partition": String(message.meta?.original?.partition ?? message.meta?.partition ?? ""),
                    "original-offset": message.meta?.original?.offset ?? message.meta?.offset ?? "",
                    "retry-attempt": String(retryData.attempt),
                    "retry-max-attempts": String(retryData.maxAttempts),
                    "retry-topic": retryData.retryTopic,
                    "dlq-topic": retryData.dlqTopic ?? ""
                }
            }]
        });

        console.log(`[RETRY-WORKER] Published retry attempt ${retryData.attempt} → ${retryData.retryTopic}`);
    }

    async publishDlq(retryData) {
        if (!retryData.dlqTopic) throw new Error("DLQ topic is required");

        const message = retryData.message;

        await this.kafkaProducer.send({
            topic: retryData.dlqTopic,
            messages: [{
                key: message.meta?.key ?? null,
                value: JSON.stringify(message.data),
                headers: {
                    "message-id": message.meta?.messageId ?? "",
                    stage: "dlq",
                    "original-topic": message.meta?.original?.topic ?? message.meta?.topic ?? "",
                    "original-partition": String(message.meta?.original?.partition ?? message.meta?.partition ?? ""),
                    "original-offset": message.meta?.original?.offset ?? message.meta?.offset ?? "",
                    "retry-attempt": String(retryData.attempt),
                    "retry-max-attempts": String(retryData.maxAttempts),
                    "retry-topic": retryData.retryTopic ?? "",
                    "dlq-topic": retryData.dlqTopic
                }
            }]
        });

        console.log(`[RETRY-WORKER] Published DLQ → ${retryData.dlqTopic}`);
    }

    calculateFailureDelay() {
        return Math.max(1000, this.pollIntervalMs);
    }
}

module.exports = RetryWorker;