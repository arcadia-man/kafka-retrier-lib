const { handleMessage } = require("../utils/message-handler");

const topic = "inventory-updated";
const retry = {
  "enabled": true,
  "topic": "inventory-updated.retry",
  "strategy": "exponential",
  "maxAttempts": 4,
  "initialDelayMs": 1000,
  "maxDelayMs": 8000
};
const dlq = {
  "enabled": true,
  "topic": "inventory-updated.dlq"
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
    failuresBeforeSuccess: 2
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
    failuresBeforeSuccess: 2
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
