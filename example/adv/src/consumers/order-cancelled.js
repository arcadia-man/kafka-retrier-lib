const { handleMessage } = require("../utils/message-handler");

const topic = "order-cancelled";
const retry = {
  "enabled": true,
  "topic": "order-cancelled.retry",
  "strategy": "exponential",
  "maxAttempts": 3,
  "initialDelayMs": 1000,
  "maxDelayMs": 5000
};
const dlq = {
  "enabled": true,
  "topic": "order-cancelled.dlq"
};

const normalConsumer = {
  name: topic + "-normal",
  stage: "normal",
  groupId: topic + "-normal-group",
  topic,
  retry,
  dlq,
  handler: (message) => handleMessage(message, {
    consumerName: topic + "-normal",
    failuresBeforeSuccess: 1
  })
};

const retryConsumer = {
  name: topic + "-retry",
  stage: "retry",
  groupId: topic + "-retry-group",
  topic: retry.topic,
  retry,
  dlq,
  handler: (message) => handleMessage(message, {
    consumerName: topic + "-retry",
    failuresBeforeSuccess: 1
  })
};

const dlqConsumer = {
  name: topic + "-dlq",
  stage: "dlq",
  groupId: topic + "-dlq-group",
  topic: dlq.topic,
  retry: { enabled: false },
  dlq: { enabled: false },
  handler: (message) => handleMessage(message, {
    consumerName: topic + "-dlq",
    failuresBeforeSuccess: 0
  })
};

module.exports = {
  normalConsumer,
  retryConsumer: retry.enabled ? retryConsumer : null,
  dlqConsumer: dlq.enabled ? dlqConsumer : null
};
