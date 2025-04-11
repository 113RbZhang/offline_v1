package com.rb.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * Package com.rb.CdcSource
 * Author runbo.zhang
 * Date 2025/4/7 20:55
 * description: 1
 */
public class SourceSinkUtils {
    public static void main(String[] args) throws Exception {

    }

    public static DataStreamSource<String> cdcRead(StreamExecutionEnvironment env, String dbName, String tableName) throws Exception {


        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
//        env.enableCheckpointing(3000);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints");


        // 设置 source 节点的并行度为 4
        // 设置 sink 节点并行度为 1


        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode", "double");
        properties.setProperty("time.precision.mode", "connect");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList(dbName) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList(dbName + "." + tableName) // 设置捕获的表
                .username("root")
                .password("root")
                .startupOptions(StartupOptions.earliest())
//                .startupOptions(StartupOptions.initial())
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        return mySQLSource;
    }

    public static KafkaSink<String> sinkToKafka(String topic_name) {
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh03:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic_name)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        return sink;


    }
    public static DataStreamSource<String>  kafkaRead(StreamExecutionEnvironment env,String topic ){

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh03:9092")
                .setTopics(topic)
                .setGroupId(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        return kafkaSource;
    }
}
