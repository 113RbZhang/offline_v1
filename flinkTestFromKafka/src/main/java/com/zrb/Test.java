import cn.hutool.json.ObjectMapper;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh03:9092")
                .setTopics("task-02")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> data = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        data.print();
        DataStreamSource<String> data2 = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                ctx.collect("{\"ds\": \"20250407\",\"ts\": \"08:15:00\",\"field1\": \"data_value_1\",\"field2\": \"100\",\"field3\": \"false\"}");
                ctx.collect("{\"ds\": \"20250407\",\"ts\": \"10:30:00\",\"field1\": \"data_value_2\",\"field2\": \"200\",\"field3\": \"true\"}");
                ctx.collect("{\"ds\": \"20250408\",\"ts\": \"13:45:00\",\"field1\": \"data_value_3\",\"field2\": \"300\",\"field3\": \"false\"}");
                ctx.collect("{\"ds\": \"20250408\",\"ts\": \"16:00:00\",\"field1\": \"data_value_4\",\"field2\": \"400\",\"field3\": \"true\"}");
                ctx.collect("{\"ds\": \"20250409\",\"ts\": \"18:15:00\",\"field1\": \"data_value_5\",\"field2\": \"500\",\"field3\": \"false\"}");
            }

            @Override
            public void cancel() {

            }
        });


        SingleOutputStreamOperator<RowData> data1 = data2.map(JSON::parseObject).map(json -> {
            String ds = json.get("ds").toString();
            String ts = json.get("ts").toString();
            String field1 = json.get("field1").toString();
            String field2 = json.get("field2").toString();
            String field3 = json.get("field3").toString();
            return GenericRowData.of(
                    StringData.fromString(ds),
                    StringData.fromString(ts),
                    StringData.fromString(field1),
                    StringData.fromString(field2),
                    StringData.fromString(field3)
            );

        });

//        data1.print();
        Configuration conf = new Configuration();
        conf.set("orc.compress", CompressionKind.SNAPPY.name());
        String orcSchema = "struct<ds:string,ts:string,field1:string,field2:string,field3:string>";

        LogicalType[] fieldTypes = new LogicalType[]{
                new VarCharType(255),    // ds: 字符串类型，长度 8（如 "20250409"）
                new VarCharType(255),    // ts: 字符串类型，长度 8（如 "18:15:00"）
                new VarCharType(255),   // field1: 字符串类型，长度 12（如 "data_value_5"）
                new VarCharType(255),         // field2: 整数类型
                new VarCharType(255)      // field3: 布尔类型
        };

        RowDataVectorizer vectorizer = new RowDataVectorizer(orcSchema,fieldTypes );
        OrcBulkWriterFactory<RowData> writerFactory = new OrcBulkWriterFactory<>(
                vectorizer,
                conf
        );
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        FileSink<RowData> sink =
//        FileSink<String> sink =
                FileSink
                .forBulkFormat(new Path("hdfs://cdh01:8020/2207A/test/2"), writerFactory)
//                .forRowFormat(new Path("hdfs://cdh01:8020/path/to/output"), new SimpleStringEncoder<String>("UTF-8"))
                .build();

//        data1.sinkTo(sink);

        data1.print();
        env.execute();


    }

}
