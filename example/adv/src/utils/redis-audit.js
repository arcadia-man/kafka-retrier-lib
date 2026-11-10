const { createClient } = require("redis");

async function auditRedis() {
  const redis = createClient({
    url: process.env.REDIS_URL || "redis://localhost:6379"
  });

  redis.on("error", error => console.error("[REDIS AUDIT ERROR]", error));

  try {
    await redis.connect();

    const scheduled = await redis.zCard("retry:scheduled");
    const processing = await redis.zCard("retry:processing");

    let dataKeys = [];
    for await (const item of redis.scanIterator({
      MATCH: "retry:data:*",
      COUNT: 100
    })) {
      if (Array.isArray(item)) {
        dataKeys.push(...item);
      } else {
        dataKeys.push(item);
      }
    }

    const report = {
      scheduledRetries: scheduled,
      processingRetries: processing,
      remainingDataKeys: dataKeys.length,
      dataKeys
    };

    console.log(JSON.stringify(report, null, 2));
    return report;
  } finally {
    if (redis.isOpen) await redis.quit();
  }
}

if (require.main === module) {
  auditRedis().catch(error => {
    console.error(error);
    process.exitCode = 1;
  });
}

module.exports = { auditRedis };
