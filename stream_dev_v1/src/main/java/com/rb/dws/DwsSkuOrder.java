package com.rb.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.utils.DateFormatUtil;
import com.rb.utils.HbaseUtil;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

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
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaRead(env, "dwd_trade_order_detail_v1");
//        kafkaRead.print();
        SingleOutputStreamOperator<JSONObject> process = kafkaRead.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {


                try {
                    out.collect(JSON.parseObject(value));
                } catch (Exception e) {
                    System.out.println("有脏数据");
                }


            }
        });
        KeyedStream<JSONObject, String> keyedStream = process.keyBy(o -> o.getString("id"));
        kafkaRead.print();
        SingleOutputStreamOperator<JSONObject> distinctDs = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> stateDescriptor = new ValueStateDescriptor<>("", JSONObject.class);
                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                JSONObject data = valueState.value();
                if (data != null) {
                    String splitOriginalAmount = data.getString("split_original_amount");
                    String splitCouponAmount = data.getString("split_coupon_amount");
                    String splitActivityAmount = data.getString("split_activity_amount");
                    String splitTotalAmount = data.getString("split_total_amount");

                    data.put("split_original_amount", "-" + splitOriginalAmount);
                    data.put("split_coupon_amount", "-" + splitCouponAmount);
                    data.put("split_activity_amount", "-" + splitActivityAmount);
                    data.put("split_total_amount", "-" + splitTotalAmount);
                    out.collect(data);
                }
                valueState.update(value);
                out.collect(value);

            }
        });


        SingleOutputStreamOperator<JSONObject> wateredDs = distinctDs.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts_ms");
                            }
                        }));
        WindowedStream<JSONObject, String, TimeWindow> windowDs = wateredDs.keyBy(o -> o.getString("sku_id"))
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)));
        SingleOutputStreamOperator<JSONObject> reduce =
                windowDs
                        .reduce(new ReduceFunction<JSONObject>() {
                                    @Override
                                    public JSONObject reduce(JSONObject v1, JSONObject v2) throws Exception {
                                        v1.put("split_original_amount", v1.getDouble("split_original_amount") + v2.getDouble("split_original_amount"));
                                        v1.put("split_coupon_amount", v1.getDouble("split_coupon_amount") + v2.getDouble("split_coupon_amount"));
                                        v1.put("split_activity_amount", v1.getDouble("split_activity_amount") + v2.getDouble("split_activity_amount"));
                                        v1.put("split_total_amount", v1.getDouble("split_total_amount") + v2.getDouble("split_total_amount"));
                                        return v1;
                                    }
                                }, new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                                    @Override
                                    public void process(String s, ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>.Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
                                        JSONObject next = elements.iterator().next();
                                        TimeWindow window = context.window();
                                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                                        next.put("stt", stt);
                                        next.put("edt", edt);
                                        next.put("curDate", curDate);
                                        out.collect(next);

                                    }
                                }
                        );
        reduce.print("lkj");

        DataStream<JSONObject> resultStream =
                AsyncDataStream.unorderedWait(reduce,
                        //如何发送异步请求
                        new RichAsyncFunction<JSONObject, JSONObject>() {
                    private AsyncConnection hbaseCon;


                            @Override
                            public void open(Configuration parameters) throws Exception {
                                hbaseCon= HbaseUtil.getHbaseAsyncCon();
                            }

                            @Override
                            public void close() throws Exception {
                                HbaseUtil.closeHbaseAsyncCon(hbaseCon);
                            }

                            @Override
                            public void asyncInvoke(JSONObject data, ResultFuture<JSONObject> resultFuture) throws Exception {
                                String skuId = data.getString("sku_id");
                                JSONObject dimAsync = HbaseUtil.readDimAsync(hbaseCon, "", "", skuId);
                                if (dimAsync!=null){
                                    data.put("sku_name", dimAsync.getString("sku_name"));
                                    data.put("spu_id", dimAsync.getString("spu_id"));
                                    data.put("category3_id", dimAsync.getString("category3_id"));
                                    data.put("tm_id", dimAsync.getString("tm_id"));
                                    //处理后的数据传入下游
                                    resultFuture.complete(Collections.singleton(data));

                                }else {
                                    System.out.println("未查询到dim维度数据，关联失败");
                                }

                            }
                        },
                        60, TimeUnit.SECONDS);
        resultStream.print();
//        AsyncRetryStrategy asyncRetryStrategy =
//                new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder(3, 100L) // maxAttempts=3, fixedDelay=100ms
//                        .ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
//                        .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
//                        .build();

//// 应用异步 I/O 转换操作并启用重试
//        DataStream<Tuple2<String, String>> resultStream =
//                AsyncDataStream.unorderedWaitWithRetry(stream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100, asyncRetryStrategy);


        env.disableOperatorChaining();
        env.execute();
    }
}
