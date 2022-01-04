package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.LookupOptions;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.source.RedisDynamicTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.streaming.connectors.redis.common.config.RedisOptions.*;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;

/**
 * Created by jeff.zou on 2020/9/10.
 */
public class RedisDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    public static final String IDENTIFIER = "redis";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        TableSchema tableSchema = context.getCatalogTable().getSchema();
        Map<String, String> options = context.getCatalogTable().getOptions();
        return new RedisDynamicTableSource(
                options,
                getLookupOptions(options),
                tableSchema,
                config
        );
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        if (context.getCatalogTable().getOptions().containsKey(REDIS_COMMAND)) {
            context.getCatalogTable().getOptions().put(REDIS_COMMAND, context.getCatalogTable().getOptions().get(REDIS_COMMAND).toUpperCase());
        }
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);
        return new RedisDynamicTableSink(context.getCatalogTable().getOptions(), context.getCatalogTable().getSchema(), config);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RedisOptions.DATABASE);
        options.add(RedisOptions.HOST);
        options.add(RedisOptions.PORT);
        options.add(RedisOptions.MAXIDLE);
        options.add(RedisOptions.MAXTOTAL);
        options.add(RedisOptions.CLUSTERNODES);
        options.add(RedisOptions.PASSWORD);
        options.add(RedisOptions.TIMEOUT);
        options.add(RedisOptions.MINIDLE);
        options.add(RedisOptions.COMMAND);
        options.add(RedisOptions.REDISMODE);
        options.add(RedisOptions.KEY_COLUMN);
        options.add(RedisOptions.VALUE_COLUMN);
        options.add(RedisOptions.FIELD_COLUMN);
        options.add(RedisOptions.PUT_IF_ABSENT);
        options.add(RedisOptions.TTL);

        options.add(CACHE_TYPE);
        options.add(CACHE_MAX_ROWS);
        options.add(CACHE_TTL);
        options.add(RedisOptions.CACHE_PENETRATION_PREVENT);
        options.add(RedisOptions.CACHE_MAX_RETRIES);

        options.add(LOOKUP_HASH_ENABLE);
        options.add(LOOKUP_REDIS_DATATYPE);
        return options;
    }

    private void validateConfigOptions(ReadableConfig config) {

    }

    public static LookupOptions getLookupOptions(Map<String, String> options) {
        Configuration tableOptions = Configuration.fromMap(options);
        LookupOptions.Builder builder = LookupOptions.builder();
        builder.setCacheType(tableOptions.getString(CACHE_TYPE));
        builder.setCacheExpireMs(((Duration) tableOptions.get(CACHE_TTL)).toMillis());
        builder.setCacheMaxRows((long) tableOptions.getInteger(CACHE_MAX_ROWS));
        builder.setMaxRetryTimes(tableOptions.getInteger(CACHE_MAX_RETRIES));
        builder.setCachePenetration(tableOptions.getBoolean(CACHE_PENETRATION_PREVENT));
        builder.setRedisDataType(tableOptions.getString(LOOKUP_REDIS_DATATYPE));

        return builder.build();
    }
}