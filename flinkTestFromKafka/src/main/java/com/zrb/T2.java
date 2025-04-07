//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
//import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
//import org.apache.flink.table.api.DataTypes;
//import org.apache.flink.table.types.DataType;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//
//import java.io.IOException;
//
//public class T2 {
//    public static void main(String[] args) throws Exception {
//        // 设置执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // Kafka 配置
//        Properties kafkaProps = new Properties();
//        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-group");
//
//        // 创建 Kafka 数据源
//        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers("localhost:9092")
//                .setTopics("your-kafka-topic")
//                .setGroupId("flink-group")
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//
//        // 从 Kafka 读取数据
//        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//
//        // 解析 JSON 数据
//        DataStream<Tuple5<String, String, String, String, String>> parsedStream = kafkaStream.map(json -> {
//            ObjectMapper mapper = new ObjectMapper();
//            try {
//                // 假设 JSON 数据包含 ds, ts 以及其他三个字段 field1, field2, field3
//                return new Tuple5<>(
//                        mapper.readTree(json).get("ds").asText(),
//                        mapper.readTree(json).get("ts").asText(),
//                        mapper.readTree(json).get("field1").asText(),
//                        mapper.readTree(json).get("field2").asText(),
//                        mapper.readTree(json).get("field3").asText()
//                );
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        });
//
//        // 定义 ORC 格式
//        DataType orcSchema = DataTypes.ROW(
//                DataTypes.FIELD("ds", DataTypes.STRING()),
//                DataTypes.FIELD("ts", DataTypes.STRING()),
//                DataTypes.FIELD("field1", DataTypes.STRING()),
//                DataTypes.FIELD("field2", DataTypes.STRING()),
//                DataTypes.FIELD("field3", DataTypes.STRING())
//        );
//
//        org.apache.hadoop.hive.serde2.typeinfo.TypeInfo orcTypeInfo = OrcSchemaConverter.convertToTypeInfo(orcSchema);
//        OrcBulkWriterFactory<Tuple5<String, String, String, String, String>> writerFactory = new OrcBulkWriterFactory<>(
//                (path, conf) -> {
//                    Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf)
//                            .setSchema(orcTypeInfo)
//                            .stripeSize(67108864)
//                            .blockSize(268435456)
//                            .compress(org.apache.hadoop.io.compress.SnappyCodec.class)
//                            .bufferSize(131072));
//                    return new org.apache.flink.formats.orc.writer.OrcRowDataWriter(writer, orcTypeInfo);
//                },
//                new Configuration()
//        );
//
//        // 配置 HDFS 输出
//        String hdfsPath = "hdfs://localhost:9000/your-output-path";
//        OutputFileConfig outputConfig = OutputFileConfig.builder()
//                .withPartPrefix("part")
//                .withPartSuffix(".orc")
//                .build();
//
//        StreamingFileSink<Tuple5<String, String, String, String, String>> sink = StreamingFileSink
//                .forBulkFormat(new Path(hdfsPath), writerFactory)
//                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd"))
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//                                .withMaxPartSize(1024 * 1024 * 1024)
//                                .build())
//                .withOutputFileConfig(outputConfig)
//                .build();
//
//        // 将数据写入 HDFS
//        parsedStream.addSink(sink);
//
//        // 执行作业
//        env.execute("Flink Kafka to HDFS ORC");
//}
