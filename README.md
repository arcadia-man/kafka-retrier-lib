# @arcadia-man/kafka-retrier

Kafka consumer retry orchestration with Redis-backed scheduling and Dead Letter Queue (DLQ) support.

[![npm version](https://img.shields.io/npm/v/@arcadia-man/kafka-retrier.svg)](https://www.npmjs.com/package/@arcadia-man/kafka-retrier)
[![npm downloads](https://img.shields.io/npm/dm/@arcadia-man/kafka-retrier.svg)](https://www.npmjs.com/package/@arcadia-man/kafka-retrier)
[![License](https://img.shields.io/npm/l/@arcadia-man/kafka-retrier.svg)](https://github.com/arcadia-man/kafka-retrier-lib/blob/main/LICENSE)

## Why this library?

Kafka consumers often need reliable retry handling when message processing fails.

This library provides reusable retry orchestration using Kafka and Redis, including retry scheduling, backoff strategies, retry state management, and Dead Letter Queue handling.

Your application remains responsible for business logic.

## Features

- Kafka producer and consumer management
- Shared Kafka connection per application process
- Fixed retry strategy
- Exponential backoff retry strategy
- Configurable maximum retry attempts
- Redis-backed retry scheduling
- Background retry worker
- Dead Letter Queue (DLQ) support
- Retry metadata
- Multiple consumers using shared Kafka infrastructure
- At-least-once delivery model

## Architecture

~~~mermaid
flowchart TD
    A[Kafka Topic] --> B[Kafka Consumer]
    B --> C[Message Handler]

    C -->|Success| D[Commit Offset]

    C -->|Failure| E[Retry Manager]
    E --> F[(Redis)]
    F --> G[Retry Worker]

    G -->|Retry Available| H[Kafka Retry Topic]
    H --> B

    G -->|Max Attempts Reached| I[Dead Letter Queue]
~~~

## Components

| Component | Responsibility |
|---|---|
| `Kafka` | Manages shared Kafka infrastructure |
| `KafkaProducer` | Publishes Kafka messages |
| `KafkaConsumer` | Consumes and processes Kafka messages |
| `RetryManager` | Handles retry scheduling |
| `RetryWorker` | Processes scheduled retry jobs |
| `RetryStore` | Stores retry state in Redis |

The application owns the business logic and message handlers.

## Installation

~~~bash
npm install @arcadia-man/kafka-retrier
~~~

## Requirements

- Node.js 18+
- Apache Kafka
- Redis 6+

## Basic Usage

~~~js
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

    console.log("Processing order:", order.orderId);

    await processOrder(order);
  });
}

main().catch(console.error);
~~~

## Retry Strategies

### Fixed Retry

Use a fixed delay when every retry should happen after the same interval.

~~~js
{
  maxAttempts: 3,
  strategy: "fixed",
  delayMs: 5000
}
~~~

### Exponential Backoff

Use exponential backoff when the delay should increase after each failure.

~~~js
{
  maxAttempts: 5,
  strategy: "exponential",
  initialDelayMs: 1000,
  maxDelayMs: 30000
}
~~~

For example, with a 1-second initial delay:

~~~text
Attempt 1 → 1s
Attempt 2 → 2s
Attempt 3 → 4s
Attempt 4 → 8s
Attempt 5 → 16s
~~~

The delay is capped by `maxDelayMs`.

## Dead Letter Queue

When a message reaches the configured maximum retry attempts, it can be sent to a Dead Letter Queue instead of being retried indefinitely.

The DLQ preserves failed messages for investigation, replay, or manual processing.

## Message Context

The consumer handler receives message data together with Kafka and retry metadata.

~~~js
await consumer.start(async (message) => {
  console.log(message.data);
  console.log(message.meta);
});
~~~

The message context can contain:

~~~text
topic
partition
offset
key
headers
retry metadata
~~~

This allows the application to make business decisions using the original Kafka message context.

## At-Least-Once Delivery

The library follows an at-least-once delivery model.

A message may therefore be processed more than once.

Business logic should be designed to handle duplicate processing safely.

For example, order processing should ideally be idempotent when the same order is received again.

## Multiple Consumers

Multiple consumers can share the same Kafka infrastructure.

~~~js
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
~~~

This avoids creating unnecessary Kafka connections for every consumer.

## Redis Retry State

Redis is used to coordinate retry scheduling and retry state.

The library maintains the retry lifecycle through Redis and a background retry worker.

Business data is not stored in the retry system.

## Development

Clone the repository:

~~~bash
git clone https://github.com/arcadia-man/kafka-retrier-lib.git
cd kafka-retrier-lib
~~~

Install dependencies:

~~~bash
npm install
~~~

Run tests:

~~~bash
npm test
~~~

Validate the npm package:

~~~bash
npm pack --dry-run
~~~

## Versioning

This project follows Semantic Versioning.

~~~text
MAJOR.MINOR.PATCH
~~~

Examples:

~~~text
1.0.0
1.0.1
1.1.0
2.0.0
~~~

Release versions are determined automatically from commit messages.

~~~text
fix: correct retry scheduling
~~~

Creates a patch release.

~~~text
feat: add exponential retry
~~~

Creates a minor release.

~~~text
feat!: change retry configuration API
~~~

Creates a major release.

Automated release commits use the `gitbot:` prefix.

Example:

~~~text
gitbot: release 1.1.0
~~~

## Repository

GitHub:

https://github.com/arcadia-man/kafka-retrier-lib

npm:

https://www.npmjs.com/package/@arcadia-man/kafka-retrier

## License

MIT