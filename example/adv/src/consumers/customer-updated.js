const { handleMessage } = require("../utils/message-handler");

const topic = "customer-updated";
const retry = {
  "enabled": false,
  "topic": "customer-updated.retry",
  "strategy": "fixed",
  "maxAttempts": 2,
  "initialDelayMs": 2000,
  "maxDelayMs": 2000
};
const dlq = {
  "enabled": false,
  "topic": "customer-updated.dlq"
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
    failuresBeforeSuccess: 0
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
    failuresBeforeSuccess: 0
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
