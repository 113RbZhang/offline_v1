import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.CompressionKind;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;


public class Test_partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints");


        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh03:9092")
                .setTopics("task-02")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> data = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        data.print();
//        DataStreamSource<String> data2 = env.addSource(new SourceFunction<String>() {
//            @Override
//            public void run(SourceContext<String> ctx) throws Exception {
//                ctx.collect("{\"ds\": \"20250407\",\"ts\": \"08:15:00\",\"field1\": \"data_value_1\",\"field2\": \"100\",\"field3\": \"false\"}");
//                ctx.collect("{\"ds\": \"20250407\",\"ts\": \"10:30:00\",\"field1\": \"data_value_2\",\"field2\": \"200\",\"field3\": \"true\"}");
//                ctx.collect("{\"ds\": \"20250408\",\"ts\": \"13:45:00\",\"field1\": \"data_value_3\",\"field2\": \"300\",\"field3\": \"false\"}");
//                ctx.collect("{\"ds\": \"20250408\",\"ts\": \"16:00:00\",\"field1\": \"data_value_4\",\"field2\": \"400\",\"field3\": \"true\"}");
//                ctx.collect("{\"ds\": \"20250409\",\"ts\": \"18:15:00\",\"field1\": \"data_value_5\",\"field2\": \"500\",\"field3\": \"false\"}");
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        });


        SingleOutputStreamOperator<RowData> data1 = data.map(JSON::parseObject).map(json -> {
            String ds = json.get("ds").toString();
            String ts = json.get("ts").toString();
            String field1 = json.get("field1").toString();
            String field2 = json.get("field2").toString();
            String field3 = json.get("field3").toString();
            return GenericRowData.of(
                    //orc要求的格式
                    StringData.fromString(ds),
                    StringData.fromString(ts),
                    StringData.fromString(field1),
                    StringData.fromString(field2),
                    StringData.fromString(field3)
            );

        });


//        data1.print();
        //orc转换和设置snappy压缩
        Configuration conf = new Configuration();
        conf.set("orc.compress", CompressionKind.SNAPPY.name());

        String orcSchema = "struct<ds:string,ts:string,field1:string,field2:string,field3:string>";

        LogicalType[] fieldTypes = new LogicalType[]{
                new VarCharType(255),    // ds: 字符串类型，长度 8（如 "18:15:00"）
                new VarCharType(255),    // ts: 字符串类型，长度 8（如 "18:15:00"）
                new VarCharType(255),   // field1: 字符串类型，长度 12（如 "data_value_5"）
                new VarCharType(255),   // field2: 整数类型
                new VarCharType(255)    // field3: 布尔类型
        };

        RowDataVectorizer vectorizer = new RowDataVectorizer(orcSchema,fieldTypes );

        OrcBulkWriterFactory<RowData> writerFactory = new OrcBulkWriterFactory<>(
                vectorizer,
                conf
        );

        //给本地hdfs写入权限
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        DefaultRollingPolicy<RowData, String> build = DefaultRollingPolicy.builder()
                .withInactivityInterval(Duration.ofMinutes(1))//文件空隙阈值 无新数据则关闭文件
                .withRolloverInterval(Duration.ofMinutes(1))//滚动时间
                .withMaxPartSize(MemorySize.parse("128", MemorySize.MemoryUnit.MEGA_BYTES))//滚动大小
                .build();
//        OnCheckpointRollingPolicy.


        FileSink<RowData> sink = FileSink
                .forBulkFormat(new Path("hdfs://cdh01:8020/2207A/test_partition/test_roll2"), writerFactory)
                .withRollingPolicy(new newCheckpointRollingPolicy())
                .withBucketAssigner(new DsBucketAssigner())
                .build();



        data1.sinkTo(sink);

        env.execute();


    }
    public static class DsBucketAssigner implements BucketAssigner<RowData ,String>{

        @Override
        public String getBucketId(RowData row, Context context) {
            StringData ds = row.getString(0);
            return "ds=" + ds.toString();
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }

    }
    public static class newCheckpointRollingPolicy extends CheckpointRollingPolicy<RowData, String> {


        private static final long MAX_FILE_SIZE = 1024 * 1024 * 128; // 128MB

        @Override
        public boolean shouldRollOnEvent(PartFileInfo<String> partFileInfo, RowData rowData) throws IOException {
            // 当文件大小超过最大文件大小时滚动文件
            return partFileInfo.getSize() >= MAX_FILE_SIZE;
        }

        @Override
        public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileInfo, long currentProcessingTime) throws IOException {
            // 当文件创建时间超过 1 分钟时滚动文件
            long oneHourInMillis = TimeUnit.MINUTES.toMillis(1);
            return currentProcessingTime - partFileInfo.getCreationTime() >= oneHourInMillis;
        }
    }




}
