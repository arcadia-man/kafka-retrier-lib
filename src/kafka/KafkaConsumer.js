const KafkaMessage = require("../message/KafkaMessage");

class KafkaConsumer {
    constructor(kafka, config, retryManager = null) {
        if (!kafka) throw new Error("Kafka instance is required");
        if (!config || typeof config !== "object") throw new Error("Kafka consumer configuration is required");
        if (!config.groupId || typeof config.groupId !== "string") throw new Error("Kafka consumer groupId is required");
        if (!config.topic || typeof config.topic !== "string") throw new Error("Kafka consumer topic is required");
        if (typeof config.handler !== "function") throw new Error("Kafka consumer handler must be a function");

        this.name = config.name;
        this.stage = config.stage ?? "normal";
        this.groupId = config.groupId;
        this.topic = config.topic;
        this.handler = config.handler;
        this.retryManager = retryManager;

        this.consumer = kafka.createConsumer({ groupId: this.groupId });

        this.connected = false;
        this.running = false;
    }

    async connect() {
        if (this.connected) return;

        await this.consumer.connect();

        await this.consumer.subscribe({ topic: this.topic, fromBeginning: false });

        this.connected = true;
    }

    async start() {
        if (!this.connected) throw new Error(`Kafka consumer "${this.name}" is not connected. Call connect() first.`);
        if (this.running) return;

        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const kafkaMessage = this.createKafkaMessage({ topic, partition, message });

                try {
                    await this.handler(kafkaMessage);
                } catch (error) {
                    await this.handleError(kafkaMessage, error);
                }
            }
        });

        this.running = true;
    }

    createKafkaMessage({ topic, partition, message }) {
        const headers = this.parseHeaders(message.headers);

        return new KafkaMessage({
            data: this.parseMessage(message.value),
            messageId: this.getMessageId(headers),
            topic,
            partition,
            offset: message.offset,
            key: this.parseKey(message.key),
            headers,
            consumer: this.name,
            stage: this.stage,
            original: this.getOriginal(headers, { topic, partition, offset: message.offset }),
            retry: this.getRetryMetadata(headers)
        });
    }

    async handleError(message, error) {
        if (!this.retryManager) throw error;

        await this.retryManager.retry(message, error);
    }

    getOriginal(headers, current) {
        if (headers["original-topic"] !== undefined && headers["original-partition"] !== undefined && headers["original-offset"] !== undefined) {
            return {
                topic: headers["original-topic"],
                partition: Number(headers["original-partition"]),
                offset: headers["original-offset"]
            };
        }

        return {
            topic: current.topic,
            partition: current.partition,
            offset: current.offset
        };
    }

    getRetryMetadata(headers) {
        if (!headers["retry-attempt"]) return null;

        return {
            enabled: true,
            attempt: Number(headers["retry-attempt"]),
            maxAttempts: Number(headers["retry-max-attempts"]),
            retryTopic: headers["retry-topic"] || null,
            dlqTopic: headers["dlq-topic"] || null
        };
    }

    parseMessage(value) {
        if (value === null || value === undefined) return null;

        const text = value.toString();

        try {
            return JSON.parse(text);
        } catch {
            return text;
        }
    }

    parseKey(key) {
        if (key === null || key === undefined) return null;

        return key.toString();
    }

    parseHeaders(headers = {}) {
        const parsedHeaders = {};

        for (const [key, value] of Object.entries(headers)) parsedHeaders[key] = value === null || value === undefined ? null : value.toString();

        return parsedHeaders;
    }

    getMessageId(headers) {
        return headers.messageId || headers["message-id"] || null;
    }

    async disconnect() {
        if (!this.connected) return;

        await this.consumer.disconnect();

        this.connected = false;
        this.running = false;
    }
}

module.exports = KafkaConsumer;