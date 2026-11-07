const { Kafka: KafkaJS } = require("kafkajs");

class Kafka {
    static instance = null;

    constructor(config) {
        if (Kafka.instance) throw new Error("Kafka instance already exists. Use Kafka.getInstance() instead.");
        if (!config || !Array.isArray(config.brokers)) throw new Error("Kafka brokers are required");

        Kafka.instance = this;

        this.client = new KafkaJS({ clientId: config.clientId, brokers: config.brokers });
        this.producer = null;
        this.consumers = new Set();
    }

    static getInstance() {
        if (!Kafka.instance) throw new Error("Kafka has not been initialized. Create the Kafka instance first.");
        return Kafka.instance;
    }

    createProducer() {
        if (!this.producer) this.producer = this.client.producer();
        return this.producer;
    }

    createConsumer({ groupId }) {
        if (!groupId || typeof groupId !== "string") throw new Error("Kafka consumer groupId is required");

        const consumer = this.client.consumer({ groupId });

        this.consumers.add(consumer);

        return consumer;
    }

    async disconnect() {
        if (this.producer) {
            await this.producer.disconnect();
            this.producer = null;
        }

        for (const consumer of this.consumers) await consumer.disconnect();

        this.consumers.clear();
        Kafka.instance = null;
    }
}

module.exports = Kafka;