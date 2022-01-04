package org.apache.flink.streaming.connectors.redis.table;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.config.RedisOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.*;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_CLUSTER;

/**
 * Created by jeff.zou on 2020/9/10.
 */
public class SQLInsertTest {

    public static final String CLUSTERNODES = "10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000,10.11.80.147:8001,10.11.80.147:9000,10.11.80.147:9001";

    @Test
    public void testNoPrimaryKeyInsertSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl =
                "create table sink_redis(" +
                        "username VARCHAR, " +
                        "passport VARCHAR" +
                        ") with ( " +
                        "   'connector'='redis', " +
                        "   'host'='127.0.0.1'," +
                        "   'port'='6379', " +
                        "   'redis-mode'='single'," +
                        "   'key-column'='username'," +
                        "   'value-column'='passport'," +
                        "   'command'='set')";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test1', 'test11'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }

    @Test
    public void testRedisDimForString() throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
        String datagen_source =
                "create table data_source(" +
                        "username VARCHAR, " +
                        "passport VARCHAR, " +
                        "pro AS PROCTIME() " +
                        ") with (          " +
                        "   'connector'='datagen', " +
                        "   'number-of-rows'='2'" +
                        "   )";
        tEnv.executeSql(datagen_source);

        String redisDimDDL =
                "create table redis_dim(" +
                        "username VARCHAR, " +
                        "passport VARCHAR" +
                        ") with ( " +
                        "   'connector'='redis', " +
                        "   'host'='127.0.0.1'," +
                        "   'port'='6379', " +
                        "   'redis-mode'='single'," +
                        "   'key-column'='username'," +
                        "   'value-column'='passport'" +
                        "   )";
        tEnv.executeSql(redisDimDDL);

        String printSinkDDL =
                "create table print_sink(" +
                        "username VARCHAR, " +
                        "passport1 VARCHAR," +
                        "passport2 VARCHAR" +
                        ") with ( " +
                        "   'connector'='print'" +
                        "   )";

        tEnv.executeSql(printSinkDDL);
        String sql =
                "insert into print_sink select data_source.username, data_source.passport, redis_dim.passport from data_source left join redis_dim FOR SYSTEM_TIME AS OF data_source.pro" +
                " on data_source.username = redis_dim.username";
        tEnv.executeSql(sql);

        String query_sql =
                "select data_source.username, data_source.passport, redis_dim.passport from data_source left join redis_dim FOR SYSTEM_TIME AS OF data_source.pro" +
                        " on data_source.username = redis_dim.username";
        TableResult tableResult = tEnv.executeSql(query_sql);
        tableResult.print();

    }

    @Test
    public void testRedisDimForArray() throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
        String datagen_source =
                "create table data_source(" +
                        "username VARCHAR, " +
                        "passport VARCHAR, " +
                        "pro AS PROCTIME() " +
                        ") with (          " +
                        "   'connector'='datagen', " +
                        "   'fields.username.kind'='sequence', " +
                        "   'fields.username.start'='3', " +
                        "   'fields.username.end'='10' " +
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

        String query_sql =
                "select * from data_source left join redis_dim FOR SYSTEM_TIME AS OF data_source.pro" +
                        " on data_source.username = redis_dim.username";
        TableResult tableResult = tEnv.executeSql(query_sql);
        tableResult.print();
    }


    @Test
    public void testSingleInsertHashClusterSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = "create table sink_redis(username VARCHAR, level varchar, age varchar) with ( 'connector'='redis', " +
                "'cluster-nodes'='" + CLUSTERNODES + "','redis-mode'='cluster','field-column'='level', 'key-column'='username', 'put-if-absent'='true'," +
                " 'value-column'='age', 'password'='******','" +
                REDIS_COMMAND + "'='" + RedisCommand.HSET + "', 'maxIdle'='2', 'minIdle'='1'  )";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test_hash', '3', '15'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }

    @Test
    public void testInsertArrayStructure() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl =
                "create table sink_redis(" +
                        "username VARCHAR, " +
                        "passport VARCHAR" +
                        ") with ( " +
                        "   'connector'='redis', " +
                        "   'host'='127.0.0.1'," +
                        "   'port'='6379', " +
                        "   'redis-mode'='single'," +
                        "   'key-column'='username'," +
                        "   'value-column'='passport'," +
                        "   'command'='RPUSH')";

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('1', '1'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }

    @Test
    public void testRedisDimForHash() throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);
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