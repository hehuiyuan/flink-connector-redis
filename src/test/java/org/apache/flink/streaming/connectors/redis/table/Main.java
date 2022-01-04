package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Main {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
        tEnv.getConfig().getConfiguration().setInteger("table.exec.resource.default-parallelism", 2);
        String datagen_source =
                "create table data_source(" +
                        "username VARCHAR, " +
                        "passport VARCHAR, " +
                        "pro AS PROCTIME() " +
                        ") with (          " +
                        "   'connector'='datagen', " +
                        "   'rows-per-second'='1'" +
                        "   )";
        tEnv.executeSql(datagen_source);

        String redisDimDDL =
                "create table redis_dim(" +
                        "username VARCHAR, " +
                        "passportall ARRAY<String>" +
                        ") with ( " +
                        "   'connector'='redis', " +
                        "   'host'='127.0.0.1'," +
                        "   'port'='6379', " +
                        "   'redis-mode'='single'," +
                        "   'key-column'='username'," +
                        "   'value-column'='passport'," +
                        "   'lookup.hash.enable'='true'," +
                        "   'lookup.redis.datatype'='SORTED_SET'" +
                        "   )";
        tEnv.executeSql(redisDimDDL);
        String printSinkDDL =
                "create table print_sink(" +
                        "username VARCHAR, " +
                        "passport VARCHAR," +
                        "passportall ARRAY<String>" +
                        ") with ( " +
                        "   'connector'='print'" +
                        "   )";

        tEnv.executeSql(printSinkDDL);
        String sql =
                "insert into print_sink select data_source.username, data_source.passport, redis_dim.passportall from data_source left join redis_dim FOR SYSTEM_TIME AS OF data_source.pro" +
                        " on data_source.username = redis_dim.username";
        tEnv.executeSql(sql);
    }
}
