package org.apache.flink.streaming.connectors.redis.common.source;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.LookupOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;

import java.util.Map;

public class RedisDynamicTableSource implements LookupTableSource {

    private final Map<String, String> properties;
    private final LookupOptions lookupOptions;
    private final TableSchema tableSchema;
    private final ReadableConfig config;

    public RedisDynamicTableSource(
            Map<String, String> properties,
            LookupOptions lookupOptions,
            TableSchema tableSchema,
            ReadableConfig config) {
        this.properties = properties;
        this.lookupOptions = lookupOptions;
        this.tableSchema = tableSchema;
        this.config = config;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return TableFunctionProvider.of(
                new RedisRowDataLookupFunction(
                        properties, lookupOptions, tableSchema.getFieldDataTypes(), config));

    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(
                properties,
                lookupOptions,
                tableSchema,
                config);
    }

    @Override
    public String asSummaryString() {
        return "Redis-dim";
    }
}
