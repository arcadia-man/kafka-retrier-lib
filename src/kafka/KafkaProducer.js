class KafkaProducer {
    constructor(kafka) {
        if (!kafka) throw new Error("Kafka instance is required");

        this.producer = kafka.createProducer();
        this.connected = false;
    }

    async connect() {
        if (this.connected) return;

        await this.producer.connect();

        this.connected = true;
    }

    async send({ topic, messages }) {
        if (!topic || typeof topic !== "string") throw new Error("Kafka producer topic is required");
        if (!Array.isArray(messages) || messages.length === 0) throw new Error("Kafka producer messages are required");
        if (!this.connected) throw new Error("Kafka producer is not connected. Call connect() first.");

        return this.producer.send({ topic, messages });
    }

    async disconnect() {
        if (!this.connected) return;

        await this.producer.disconnect();

        this.connected = false;
    }
}

module.exports = KafkaProducer;