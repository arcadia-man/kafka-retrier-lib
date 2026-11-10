const logger = require("./logger");

async function handleMessage(message, options = {}) {
  const {
    consumerName = "unknown",
    failuresBeforeSuccess = 0
  } = options;

  const attempt = Number(message.retry?.attempt ?? message.meta?.retry?.attempt ?? 0);
  const stage = message.stage ?? message.meta?.stage ?? "normal";

  const details = {
    consumer: consumerName,
    stage,
    topic: message.topic ?? message.meta?.topic,
    partition: message.partition ?? message.meta?.partition,
    offset: message.offset ?? message.meta?.offset,
    key: message.key ?? message.meta?.key,
    attempt,
    metadata: message.meta,
    payload: message.data ?? message
  };

  logger.info("MESSAGE RECEIVED", details);

  if (attempt < failuresBeforeSuccess) {
    const error = new Error(
      `${consumerName}: simulated failure ${attempt + 1}/${failuresBeforeSuccess}`
    );

    logger.error("SIMULATED FAILURE", {
      ...details,
      error: error.message
    });

    throw error;
  }

  logger.info("MESSAGE PROCESSED SUCCESSFULLY", details);
  return true;
}

module.exports = { handleMessage };
