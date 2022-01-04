package org.apache.flink.streaming.connectors.redis.common.config;

public class LookupOptions {
    private final String cacheType;
    private final long cacheMaxRows;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final boolean cachePenetration;
    private final String redisDataType;

    private LookupOptions(String cacheType, long cacheMaxRows, long cacheExpireMs, int maxRetryTimes, boolean cachePenetration, String redisDataType) {
        this.cacheType = cacheType;
        this.cacheMaxRows = cacheMaxRows;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
        this.cachePenetration = cachePenetration;
        this.redisDataType = redisDataType;
    }

    public String getCacheType() {
        return this.cacheType;
    }

    public long getCacheMaxRows() {
        return this.cacheMaxRows;
    }

    public long getCacheExpireMs() {
        return this.cacheExpireMs;
    }

    public int getMaxRetryTimes() {
        return this.maxRetryTimes;
    }

    public boolean isCachePenetration() {
        return this.cachePenetration;
    }

    public String getRedisDataType() {
        return redisDataType;
    }

    public static LookupOptions.Builder builder() {
        return new LookupOptions.Builder();
    }

    public static class Builder {
        private String cacheType;
        private long cacheMaxRows = -1L;
        private long cacheExpireMs = -1L;
        private int maxRetryTimes = 3;
        private boolean cachePenetration = false;
        private String redisDataType;

        public Builder() {
        }

        public LookupOptions.Builder setCacheType(String cacheType) {
            this.cacheType = cacheType;
            return this;
        }

        public LookupOptions.Builder setCacheMaxRows(long cacheMaxRows) {
            this.cacheMaxRows = cacheMaxRows;
            return this;
        }

        public LookupOptions.Builder setCacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }

        public LookupOptions.Builder setMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public LookupOptions.Builder setCachePenetration(boolean cachePenetration) {
            this.cachePenetration = cachePenetration;
            return this;
        }

        public LookupOptions.Builder setRedisDataType(String redisDataType) {
            this.redisDataType = redisDataType;
            return this;
        }

        public LookupOptions build() {
            return new LookupOptions(this.cacheType, this.cacheMaxRows, this.cacheExpireMs, this.maxRetryTimes, this.cachePenetration, this.redisDataType);
        }
    }
}