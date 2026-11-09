class RetryStore {
    static keys = {
        RETRY_SCHEDULED_KEY: "retry:scheduled",
        RETRY_PROCESSING_KEY: "retry:processing",
        RETRY_DATA_PREFIX: "retry:data:",
    };

    constructor(redis) {
        if (!redis) {
            throw new Error("Redis client is required");
        }

        this.redis = redis;
    }

    getDataKey(retryId) {
        return `${RetryStore.keys.RETRY_DATA_PREFIX}${retryId}`;
    }

    async save(retryId, retryData) {
        if (!retryId) {
            throw new Error("Retry ID is required");
        }

        if (!retryData) {
            throw new Error("Retry data is required");
        }

        await this.redis.set(
            this.getDataKey(retryId),
            JSON.stringify(retryData)
        );
    }

    async get(retryId) {
        if (!retryId) {
            return null;
        }

        const data = await this.redis.get(
            this.getDataKey(retryId)
        );

        if (!data) {
            return null;
        }

        return JSON.parse(data);
    }

    async schedule(retryId, nextRetryTime) {
        if (!retryId) {
            throw new Error("Retry ID is required");
        }

        if (!Number.isFinite(nextRetryTime)) {
            throw new Error(
                "Next retry time must be a valid timestamp"
            );
        }

        await this.redis.zAdd(
            RetryStore.keys.RETRY_SCHEDULED_KEY,
            {
                score: nextRetryTime,
                value: retryId,
            }
        );
    }

    async getDueRetries(currentTime, limit) {
        if (!Number.isFinite(currentTime)) {
            throw new Error(
                "Current time must be a valid timestamp"
            );
        }

        if (!Number.isInteger(limit) || limit <= 0) {
            throw new Error(
                "Limit must be a positive integer"
            );
        }

        return this.redis.zRangeByScore(
            RetryStore.keys.RETRY_SCHEDULED_KEY,
            0,
            currentTime,
            {
                LIMIT: {
                    offset: 0,
                    count: limit,
                },
            }
        );
    }

    async claim(retryId, leaseUntil) {
        if (!retryId) {
            throw new Error("Retry ID is required");
        }

        if (!Number.isFinite(leaseUntil)) {
            throw new Error(
                "Lease expiration must be a valid timestamp"
            );
        }

        const script = `
            if redis.call("ZSCORE", KEYS[1], ARGV[1]) == false then
                return 0
            end

            redis.call("ZREM", KEYS[1], ARGV[1])
            redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])

            return 1
        `;

        const result = await this.redis.eval(script, {
            keys: [
                RetryStore.keys.RETRY_SCHEDULED_KEY,
                RetryStore.keys.RETRY_PROCESSING_KEY,
            ],
            arguments: [
                retryId,
                String(leaseUntil),
            ],
        });

        return Number(result) === 1;
    }

    async complete(retryId) {
        if (!retryId) {
            return;
        }

        await this.redis.zRem(
            RetryStore.keys.RETRY_SCHEDULED_KEY,
            retryId
        );

        await this.redis.zRem(
            RetryStore.keys.RETRY_PROCESSING_KEY,
            retryId
        );

        await this.redis.del(
            this.getDataKey(retryId)
        );
    }

    async release(retryId, nextRetryTime) {
        if (!retryId) {
            throw new Error("Retry ID is required");
        }

        if (!Number.isFinite(nextRetryTime)) {
            throw new Error(
                "Next retry time must be a valid timestamp"
            );
        }

        await this.redis.zRem(
            RetryStore.keys.RETRY_PROCESSING_KEY,
            retryId
        );

        await this.redis.zAdd(
            RetryStore.keys.RETRY_SCHEDULED_KEY,
            {
                score: nextRetryTime,
                value: retryId,
            }
        );
    }

    async recoverExpired(currentTime, limit) {
        if (!Number.isFinite(currentTime)) {
            throw new Error(
                "Current time must be a valid timestamp"
            );
        }

        if (!Number.isInteger(limit) || limit <= 0) {
            throw new Error(
                "Limit must be a positive integer"
            );
        }

        const script = `
            local expired = redis.call(
                "ZRANGEBYSCORE",
                KEYS[1],
                "-inf",
                ARGV[1],
                "LIMIT",
                "0",
                ARGV[2]
            )

            for _, retryId in ipairs(expired) do
                redis.call(
                    "zrem",
                    KEYS[1],
                    retryId
                )

                redis.call(
                    "zadd",
                    KEYS[2],
                    ARGV[1],
                    retryId
                )
            end

            return expired
        `;

        return this.redis.eval(script, {
            keys: [
                RetryStore.keys.RETRY_PROCESSING_KEY,
                RetryStore.keys.RETRY_SCHEDULED_KEY,
            ],
            arguments: [
                String(currentTime),
                String(limit),
            ],
        });
    }
}

module.exports = RetryStore;