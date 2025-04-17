package com.rb.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.flink.util.Collector;

/**
 * @Package com.rb.dws.DwsSkuOrder
 * @Author runbo.zhang
 * @Date 2025/4/16 15:28
 * @description:
 */
public class DwsSkuOrder {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
//        env.enableCheckpointing(3000);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints");
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaRead(env, "trade_order_detail");

        SingleOutputStreamOperator<JSONObject> process = kafkaRead.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {


                try {
                    out.collect(JSON.parseObject(value));
                } catch (Exception e) {
                    System.out.println("有空数据");
                }


            }
        });
        KeyedStream<JSONObject, String> keyedStream = process.keyBy(o -> o.getString("id"));
        kafkaRead.print();
//        keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
//            private ValueState<String> valueState;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("", String.class);
//                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
//                valueState=getRuntimeContext().getState(stateDescriptor);
//            }
//
//            @Override
//            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
//
//                String data = valueState.value();
//                if (data)
//            }
//        })
//        AsyncRetryStrategy asyncRetryStrategy =
//                new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder(3, 100L) // maxAttempts=3, fixedDelay=100ms
//                        .ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
//                        .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
//                        .build();
//
//// 应用异步 I/O 转换操作并启用重试
//        DataStream<Tuple2<String, String>> resultStream =
//                AsyncDataStream.unorderedWaitWithRetry(stream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100, asyncRetryStrategy);


        env.disableOperatorChaining();
        env.execute();
    }
}
