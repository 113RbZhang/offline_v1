package com.rb.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.CdcSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static com.rb.CdcSource.kafkaRead;

/**
 * @Package com.rb.dim.Dim
 * @Author runbo.zhang
 * @Date 2025/4/8 18:59
 * @description: 1
 */
public class Dim {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dbData = CdcSource.kafkaRead(env, "online_flink_retail", "*");
        DataStreamSource<String> processData = CdcSource.kafkaRead(env, "online_flink_retail_process", "table_process_dim");

//        dbData.print("dbData");
//        processData.print("processData");
        //广播流
        MapStateDescriptor<String, String> dimRuleStateDescriptor = new MapStateDescriptor<>(
                "online_flink_retail_process",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<String>() {}));
        //声明为广播流
        BroadcastStream<String> broadcastStream = processData.broadcast(dimRuleStateDescriptor);
        //cdc流连接广播流
        BroadcastConnectedStream<String, String> connectedStream = dbData.connect(broadcastStream);

        OutputTag<String> dwdOutputTag = new OutputTag<String>("dwd_table") {
        };
        //通过广播流过滤维度表
        SingleOutputStreamOperator<Tuple2<String, String>> dimTablesStream = connectedStream.process(new BroadcastProcessFunction<String, String, Tuple2<String, String>>() {
            @Override
            public void processElement(String value, BroadcastProcessFunction<String, String, Tuple2<String, String>>.ReadOnlyContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
                JSONObject object = JSON.parseObject(value);
                String table = object.getJSONObject("source").getString("table");
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(dimRuleStateDescriptor);
                String process = broadcastState.get(table);
                String after = object.getString("after");
                if (process != null) {//获取到配置信息则是维度表
                    out.collect(Tuple2.of(after, process));
                }else {
                    ctx.output(dwdOutputTag, value);
                }
            }

            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
                JSONObject dim_process = JSON.parseObject(value);
                String op = dim_process.getString("op");

                String table = dim_process.getJSONObject("after").getString("source_table");
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(dimRuleStateDescriptor);
                if ("d".equals(op)) {//如果是删除则删除这个表
                    broadcastState.remove(table);
                } else {
                    //如果不是则添加
                    broadcastState.put(table, value);
                }

            }
        });
        SideOutputDataStream<String> sideOutput = dimTablesStream.getSideOutput(dwdOutputTag);

        dimTablesStream.print("zhuliu");

//        data.broadcast()

//        dimTablesStream.addSink(new SinkFunction<Tuple2<String, String>>() {
//            @Override
//            public void invoke(Tuple2<String, String> tp) throws Exception {
//                String data = tp.f0;
//                String process = tp.f1;
//
//
//            }
//        })


        env.disableOperatorChaining();
        env.execute("Print MySQL Snapshot + Binlog111111");
    }
}
