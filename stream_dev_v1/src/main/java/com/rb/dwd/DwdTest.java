package com.rb.dwd;

import lombok.SneakyThrows;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.rb.dwd.DwdTest
 * @Author runbo.zhang
 * @Date 2025/4/10 22:09
 * @description:
 */
public class DwdTest {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.execute("ljklkjk");
    }
}
