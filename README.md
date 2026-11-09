# @arcadia-man/kafka-retrier

Kafka consumer retry orchestration with Redis-backed scheduling and Dead Letter Queue (DLQ) support.

[![npm version](https://img.shields.io/npm/v/@arcadia-man/kafka-retrier.svg)](https://www.npmjs.com/package/@arcadia-man/kafka-retrier)
[![npm downloads](https://img.shields.io/npm/dm/@arcadia-man/kafka-retrier.svg)](https://www.npmjs.com/package/@arcadia-man/kafka-retrier)
[![License](https://img.shields.io/npm/l/@arcadia-man/kafka-retrier.svg)](https://github.com/arcadia-man/kafka-retrier-lib/blob/main/LICENSE)

## Why this library?

Kafka consumers commonly need reliable retry handling when message processing fails.

Instead of implementing retry scheduling, backoff calculation, retry state, and DLQ handling inside every service, this library provides a reusable orchestration layer using:

- Kafka for message consumption and publishing
- Redis for retry scheduling and state
- Retry workers for delayed message processing
- DLQ for messages that exceed the configured retry limit

Your application remains responsible for business logic.

## Features

- Kafka producer and consumer management
- Shared Kafka connection per application process
- Fixed retry strategy
- Exponential backoff retry strategy
- Maximum retry attempts
- Redis-backed retry scheduling
- Background retry worker
- Dead Letter Queue (DLQ)
- Retry metadata
- Multiple consumers using one shared Kafka connection
- At-least-once delivery model

## Architecture

    Kafka Topic
         |
         v
    KafkaConsumer
         |
         v
    Message Handler
      /       \
   Success   Failure
     |          |
     v          v
   Commit    RetryManager
                |
                v
              Redis
                |
                v
           RetryWorker
             /     \
            /       \
     Retry Topic    DLQ
          |
          v
     KafkaConsumer

### Responsibilities

| Component | Responsibility |
|---|---|
| `Kafka` | Shared Kafka connection |
| `KafkaProducer` | Publish Kafka messages |
| `KafkaConsumer` | Consume and process messages |
| `RetryManager` | Create and schedule retry jobs |
| `RetryWorker` | Process scheduled retry jobs |
| `RetryStore` | Store retry state in Redis |

The application owns the business logic and message handlers.

## Installation

    npm install @arcadia-man/kafka-retrier

## Requirements

- Node.js 18+
- Apache Kafka
- Redis 6+

## Configuration

    const config = {
      kafka: {
        clientId: "order-service",
        brokers: ["localhost:9092"],
      },

      redis: {
        url: "redis://localhost:6379",
      },
    };

## Basic Usage

    const {
      Kafka,
      KafkaConsumer,
      KafkaProducer,
      RetryStore,
      RetryManager,
      RetryWorker,
    } = require("@arcadia-man/kafka-retrier");

### 1. Create Kafka connection

Create one `Kafka` instance per application process.

    const kafka = new Kafka(config.kafka);

### 2. Create producer

    const producer = kafka.createProducer();

    await producer.connect();

    await producer.send({
      topic: "order-created",
      messages: [
        {
          key: "order-123",
          value: JSON.stringify({
            orderId: "order-123",
          }),
        },
      ],
    });

### 3. Create retry components

    const retryStore = new RetryStore(redis);

    const retryManager = new RetryManager({
      retryStore,
      retryConfig: {
        maxAttempts: 3,
        strategy: "exponential",
        initialDelayMs: 1000,
        maxDelayMs: 30000,
      },
    });

    const retryWorker = new RetryWorker({
      retryStore,
      producer,
    });

    await retryWorker.start();

### 4. Create consumer

    const consumer = kafka.createConsumer({
      groupId: "order-service",
      retryManager,
    });

    await consumer.connect();

    await consumer.subscribe({
      topic: "order-created",
      fromBeginning: false,
    });

    await consumer.start(async (message) => {
      const order = JSON.parse(message.data);

      console.log("Processing order:", order.orderId);

      // Your business logic
    });

## Retry on Failure

When the message handler throws an error, the library schedules the message for retry.

    await consumer.start(async (message) => {
      const order = JSON.parse(message.data);

      await processOrder(order);
    });

The flow is:

    Kafka
      |
      v
    Consumer
      |
      v
    Message Handler
      |
    Failure
      |
      v
    RetryManager
      |
      v
    Redis
      |
      v
    RetryWorker
      |
      v
    Retry Topic
      |
      v
    Consumer

The application does not need to implement its own retry timer or retry queue.

## Retry Strategies

### Fixed Retry

Use a fixed delay when every retry should happen after the same interval.

    {
      maxAttempts: 3,
      strategy: "fixed",
      delayMs: 5000
    }

Result:

    Attempt 1 → 5s
    Attempt 2 → 5s
    Attempt 3 → 5s

### Exponential Backoff

Use exponential backoff when retry delays should increase after each failure.

    {
      maxAttempts: 5,
      strategy: "exponential",
      initialDelayMs: 1000,
      maxDelayMs: 30000
    }

Example:

    Attempt 1 → 1s
    Attempt 2 → 2s
    Attempt 3 → 4s
    Attempt 4 → 8s
    Attempt 5 → 16s

The delay is capped by `maxDelayMs`.

## Dead Letter Queue

When a message reaches the maximum configured retry attempts, it can be sent to a DLQ instead of being retried indefinitely.

    Original Topic
          |
          v
       Consumer
          |
       Failure
          |
        Retry
          |
        Retry
          |
     Max Attempts
          |
          v
         DLQ

Configure the retry and DLQ topics according to your consumer configuration.

## Message Context

The consumer handler receives the message data together with Kafka and retry metadata.

    await consumer.start(async (message) => {
      console.log(message.data);
      console.log(message.meta);
    });

Example metadata:

    {
      topic,
      partition,
      offset,
      key,
      headers,
      retry
    }

The retry metadata is available when the message is processed through the retry flow.

## At-Least-Once Delivery

The library follows an at-least-once delivery model.

A message may therefore be processed more than once.

Your business logic should be designed to handle duplicate processing safely.

For example:

    await processOrder(order);

should ideally be idempotent when the same order is processed again.

## Multiple Consumers

Multiple consumers can share the same Kafka connection.

    const kafka = new Kafka({
      clientId: "order-service",
      brokers: ["localhost:9092"],
    });

    const orderConsumer = kafka.createConsumer({
      groupId: "order-service",
    });

    const paymentConsumer = kafka.createConsumer({
      groupId: "payment-service",
    });

This avoids creating a separate Kafka connection for every consumer.

## Complete Example

    const {
      Kafka,
      RetryStore,
      RetryManager,
      RetryWorker,
    } = require("@arcadia-man/kafka-retrier");

    const { createClient } = require("redis");

    async function main() {
      const kafka = new Kafka({
        clientId: "order-service",
        brokers: ["localhost:9092"],
      });

      const redis = createClient({
        url: "redis://localhost:6379",
      });

      await redis.connect();

      const producer = kafka.createProducer();

      await producer.connect();

      const retryStore = new RetryStore(redis);

      const retryManager = new RetryManager({
        retryStore,
        retryConfig: {
          maxAttempts: 3,
          strategy: "exponential",
          initialDelayMs: 1000,
          maxDelayMs: 30000,
        },
      });

      const retryWorker = new RetryWorker({
        retryStore,
        producer,
      });

      await retryWorker.start();

      const consumer = kafka.createConsumer({
        groupId: "order-service",
        retryManager,
      });

      await consumer.connect();

      await consumer.subscribe({
        topic: "order-created",
        fromBeginning: false,
      });

      await consumer.start(async (message) => {
        const order = JSON.parse(message.data);

        console.log("Processing:", order.orderId);

        await processOrder(order);
      });
    }

    main().catch(console.error);

## Redis Retry State

The retry system maintains retry state in Redis.

The retry workflow is:

    RetryManager
         |
         v
    Redis retry state
         |
         v
    Scheduled retry
         |
         v
    RetryWorker
         |
         v
    Kafka retry topic

Redis is used for scheduling and coordinating retry work rather than storing business data.

## Development

Clone the repository:

    git clone https://github.com/arcadia-man/kafka-retrier-lib.git
    cd kafka-retrier-lib

Install dependencies:

    npm install

Run tests:

    npm test

Validate the package:

    npm pack --dry-run

## Versioning

This project follows Semantic Versioning.

    MAJOR.MINOR.PATCH

Examples:

    1.0.0
    1.0.1
    1.1.0
    2.0.0

Release types are determined automatically from commit messages.

    fix: correct retry scheduling

    PATCH

    feat: add exponential retry

    MINOR

    feat!: change retry configuration API

    MAJOR

Release commits generated by CI are prefixed with:

    gitbot:

For example:

    gitbot: release v1.1.0

## Repository

GitHub:

https://github.com/arcadia-man/kafka-retrier-lib

npm:

https://www.npmjs.com/package/@arcadia-man/kafka-retrier

## License

MIT