class KafkaMessage {
    constructor({ data, messageId, topic, partition, offset, key, headers, consumer, stage, original, retry }) {
        this.data = data;

        this.meta = {
            messageId,
            topic,
            partition,
            offset,
            key,
            headers,
            consumer,
            stage,
            original,
            retry
        };
    }

    get messageId() {
        return this.meta.messageId;
    }

    get topic() {
        return this.meta.topic;
    }

    get partition() {
        return this.meta.partition;
    }

    get offset() {
        return this.meta.offset;
    }

    get key() {
        return this.meta.key;
    }

    get headers() {
        return this.meta.headers;
    }

    get consumer() {
        return this.meta.consumer;
    }

    get stage() {
        return this.meta.stage;
    }

    get original() {
        return this.meta.original;
    }

    get retry() {
        return this.meta.retry;
    }
}

module.exports = KafkaMessage;