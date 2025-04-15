package com.rb.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rb.bean.CartUu;
import com.rb.utils.DateFormatUtil;
import com.rb.utils.SourceSinkUtils;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Package com.rb.dws.DwsTest
 * @Author runbo.zhang
 * @Date 2025/4/14 10:25
 * @description:
 */
public class DwsTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints");
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStreamSource<String> kafkaRead = SourceSinkUtils.kafkaRead(env, "dwd_cart_add");
        kafkaRead.print();
        SingleOutputStreamOperator<JSONObject> JsonObj = kafkaRead.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> waterData = JsonObj.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        SingleOutputStreamOperator<JSONObject> uuDs = waterData.keyBy(o -> o.getString("user_id")).process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<String> laseCartDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor =
                        new ValueStateDescriptor<>("laseCartDate", String.class);
                laseCartDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                String lastDate = laseCartDateState.value();
                Long ts = value.getLong("ts");
                String date = DateFormatUtil.tsToDate(ts);
                if (StringUtils.isEmpty(lastDate) || !date.equals(lastDate)) {
                    out.collect(value);
                    laseCartDateState.update(lastDate);
                }
            }
        });
//        waterData.print();
        AllWindowedStream<JSONObject, TimeWindow> windowDs = uuDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<JSONObject> aDs =
                windowDs.
                        aggregate(new AggregateFunction<JSONObject, Long, Long>() {
                                      @Override
                                      public Long createAccumulator() {
                                          return 0L;
                                      }

                                      @Override
                                      public Long add(JSONObject value, Long accumulator) {
                                          return ++accumulator;
                                      }

                                      @Override
                                      public Long getResult(Long accumulator) {
                                          return accumulator;
                                      }

                                      @Override
                                      public Long merge(Long a, Long b) {
                                          return null;
                                      }
                                  }, new AllWindowFunction<Long, JSONObject, TimeWindow>() {
                                      @Override
                                      public void apply(TimeWindow window, Iterable<Long> values, Collector<JSONObject> out) throws Exception {

                                          Long cartUUCount = values.iterator().next();
                                          String start = DateFormatUtil.tsToDateTime(window.getStart());
                                          String date = DateFormatUtil.tsToDate(window.getStart());
                                          String end = DateFormatUtil.tsToDateTime(window.getEnd());
                                          JSONObject object = new JSONObject();
                                          object.put("start", start);
                                          object.put("end", end);
                                          object.put("date", date);
                                          object.put("cartUUCount", cartUUCount);
                                          out.collect(object);


                                      }
                                  }
                        );
        aDs.print();


        env.execute();

    }
}
