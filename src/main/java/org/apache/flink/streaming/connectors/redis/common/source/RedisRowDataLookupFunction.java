package org.apache.flink.streaming.connectors.redis.common.source;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.connectors.redis.common.config.LookupOptions;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.hanlder.FlinkJedisConfigHandler;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.StringUtils;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

public class RedisRowDataLookupFunction extends TableFunction<RowData> {
    private static Logger LOG = LoggerFactory.getLogger(RedisRowDataLookupFunction.class);

    private static final long serialVersionUID = 8020689154682653762L;
    public static final String HEAP = "heap";
    public static final String OFF_HEAP = "off-heap";

    private final DataType[] dataTypes;
    private final Map<String, String> properties;
    private final ReadableConfig config;
    private FlinkJedisConfigBase flinkJedisConfigBase;
    //private RedisMapper redisMapper;
    private RedisCommandsContainer redisCommandsContainer;

    private final String cacheType;
    private final long cacheMaxRows;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final boolean cachePenetration;
    private final String redisDataType;

    private boolean enableCache;
    private transient Cache<String, GenericRowData> heapCache;
    private transient OHCache<String, CacheGenericRowData> offHeapCache;

    public RedisRowDataLookupFunction(
            Map<String, String> properties, LookupOptions lookupOptions, DataType[] dataTypes, ReadableConfig config) {
        this.properties = properties;
        this.dataTypes = dataTypes;
        this.cacheType = lookupOptions.getCacheType();
        this.cacheMaxRows = lookupOptions.getCacheMaxRows();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
        this.cachePenetration = lookupOptions.isCachePenetration();
        this.redisDataType = lookupOptions.getRedisDataType();
        this.config = config;
    }

    @Override
    public void open(FunctionContext context) {
       /* redisMapper = RedisHandlerServices
                .findRedisHandler(RedisMapperHandler.class, properties)
                .createRedisMapper(config);*/
        flinkJedisConfigBase = RedisHandlerServices
                .findRedisHandler(FlinkJedisConfigHandler.class, properties).createFlinkJedisConfig(config);
        try {
            this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
            this.redisCommandsContainer.open();
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw new RuntimeException(e);
        }

        if (cacheMaxRows > 0 && cacheExpireMs > 0) {
            if (HEAP.equals(cacheType)) {
                this.heapCache =
                        CacheBuilder.newBuilder()
                                .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                                .maximumSize(cacheMaxRows)
                                .build();
                this.enableCache = true;
            } else if (OFF_HEAP.equals(cacheType)) {
                OHCacheBuilder<String, CacheGenericRowData> ohCacheBuilder =
                        OHCacheBuilder.newBuilder();
                this.offHeapCache =
                        ohCacheBuilder
                                .keySerializer(new CacheStringSerializer())
                                .valueSerializer(new CacheGenericRowDataSerializer())
                                .defaultTTLmillis(cacheExpireMs)
                                .timeouts(true)
                                .capacity(cacheMaxRows)
                                .throwOOME(true)
                                .build();
                this.enableCache = true;
            }
        }
    }

    public void eval(Object key) {
        String dbKey = String.valueOf(key);
        DataType valDataType = dataTypes[1];
        LogicalType valLogicalType = valDataType.getLogicalType();
        if (enableCache) {
            GenericRowData cacheRow =
                    cacheType.equals(HEAP)
                            ? heapCache.getIfPresent(dbKey)
                            : CacheGenericRowData.toRowData(offHeapCache.get(dbKey));
            if (cacheRow != null) {
                if (cacheRow.getArity() == 0 && cachePenetration)
                    return;
                collect(cacheRow);
            } else {
                for (int retry = 1; retry <= maxRetryTimes; retry++) {
                    try {
                        GenericRowData row = getRow(dbKey, valLogicalType);
                        if (row != null) {
                            if (cacheType.equals(HEAP)) {
                                heapCache.put(dbKey, row);
                            } else {
                                offHeapCache.put(dbKey, CacheGenericRowData.fromRowData(row));
                            }
                            collect(row);
                        } else if (cachePenetration) {
                            if (cacheType.equals(HEAP)) {
                                heapCache.put(dbKey, new GenericRowData(0));
                            } else {
                                offHeapCache.put(
                                        dbKey,
                                        new CacheGenericRowData(0));
                            }
                        }
                        break;
                    } catch (Exception e) {
                        if (retry >= maxRetryTimes) {
                            throw new RuntimeException("Execution of get jimdb cache failed.", e);
                        }
                    }
                }
            }
        } else {
            GenericRowData row = getRow(dbKey, valLogicalType);
            if (row != null) {
                collect(row);
            }
        }
    }

    private GenericRowData getRow(String dbKey, LogicalType valLogicalType) {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, StringData.fromString(dbKey));
        boolean emit = false;
        if (valLogicalType instanceof VarCharType) {
            String value = redisCommandsContainer.get(dbKey);
            if (value != null && !value.isEmpty()) {
                row.setField(1, StringData.fromString(value));
                emit = true;
            }
        } else if (valLogicalType instanceof MapType) {
            Map<String, String> value = redisCommandsContainer.hGetAll(dbKey);
            if (value != null && !value.isEmpty()) {
                row.setField(1, value);
                emit = true;
            }
        } else if (valLogicalType instanceof ArrayType) {
            if (redisDataType.equalsIgnoreCase(RedisDataType.LIST.name())) {
                List<String> value = redisCommandsContainer.lRangeAll(dbKey);
                if (value != null && !value.isEmpty()) {
                    row.setField(1, getArrayData(value));
                    emit = true;
                }
            } else if (redisDataType.equalsIgnoreCase(RedisDataType.SET.name())) {
                Set<String> value = redisCommandsContainer.sMembers(dbKey);
                if (value != null && !value.isEmpty()) {
                    row.setField(1, getArrayData(value));
                    emit = true;
                }
            } else if (redisDataType.equalsIgnoreCase(RedisDataType.SORTED_SET.name())) {
                Set<String> value = redisCommandsContainer.zRangeAll(dbKey);
                if (value != null && !value.isEmpty()) {
                    row.setField(1, getArrayData(value));
                    emit = true;
                }
            }
        }

        if (emit) {
            return row;
        }
        return null;
    }

    public ArrayData getArrayData(Collection<String> data) {
        StringData[] res = new StringData[data.size()];
        for (int i = 0; i < data.size(); i++) {
            res[i] = StringData.fromString(data.toArray()[i].toString());
        }
        return new GenericArrayData(res);
    }

    @Override
    public void close() {
        if (redisCommandsContainer != null) {
            try {
                redisCommandsContainer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
