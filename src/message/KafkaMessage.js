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
}

module.exports = KafkaMessage;