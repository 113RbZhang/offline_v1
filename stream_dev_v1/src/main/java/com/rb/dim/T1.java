//package com.rb.dim;
//
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.java.tuple.Tuple2;
//
///**
// * @Package com.rb.dim.T1
// * @Author runbo.zhang
// * @Date 2025/4/8 21:32
// * @description:
// */
//public class T1 {
//    public static void main(String[] args) {
//
//
//
//        //TODO 8.将配置流中的配置信息进行广播--broadcast
//        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcessDim.class);
//        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);
//
//        //TODO 9.将主流业务数据和广播流配置信息进行关联--connect
//        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);
//
//
////        1> {"op":"u","before":{"tm_name":"Redmi","create_time":1639440000000,"logo_url":"","id":1},"after":{"tm_name":"Redmi","create_time":1639440000000,"logo_url":"123321","id":1},"source":{"thread":1776,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000006","connector":"mysql","pos":1692,"name":"mysql_binlog_source","row":0,"ts_ms":1744112604000,"snapshot":"false","db":"gmall2024","table":"base_trademark"},"ts_ms":1744112603540}
//        jsonObjDS.print("2-->");
//
//        //TODO 10.处理关联后的数据（判断是否为维度）
//        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS.process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>() {
//            //处理主流业务数据根据维度表名到广播状态中读取配置信息，判断是否为维度
//            @Override
//            public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject,TableProcessDim>> collector) throws Exception {
//                //获取处理数据的表明
//                String table = jsonObject.getJSONObject("source").getString("table");
//
//                //获取广播状态
//                ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
//
//                //根据表明到广播状态中获取相应的配置
//                TableProcessDim tableProcessDim = broadcastState.get(table);
//
//                if (tableProcessDim!=null){
//                    //如果根据表明获取到了配置信息,说明当前处理的是维度数据
//
//                    //将维度数据继续向下游传递
//                    JSONObject data = jsonObject.getJSONObject("after");
//
//                    //在向下游传递数据,过滤不需要传递的属性
//                    String sinkColumns = tableProcessDim.getSinkColumns();
//                    deleteNotNeedColumns(data,sinkColumns);
//
//                    //在向下游传递数据,不充维度数据的操作类型属性
//                    String type = jsonObject.getString("op");
//                    data.put("op",type);
//
//                    collector.collect(Tuple2.of(data,tableProcessDim));
//                }
//            }
//            //处理广播流配智信息 v:一个配理对象将配胃数据放到广播状会中 k:维度表名
//            @Override
//            public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.Context context, Collector<Tuple2<JSONObject,TableProcessDim>> collector) throws Exception {
//                //获取操作状态
//                String op = tp.getOp();
//
//                //获取广播状态
//                BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);
//
//                //获取维度表的名称
//                String sourceTable = tp.getSourceTable();
//
//                if ("d".equals(op)){
//                    //从配置表中删除了一条数据，将对应的配置信息从广播状态中删除
//                    broadcastState.remove(sourceTable);
//                }else {
//                    //对配置表进行了读取,添加,更新, 将最新的配置信息放到广播状态中
//                    broadcastState.put(sourceTable,tp);
//                }
//            }
//        });
//
//        dimDS.print(" -->")
//    }
//}
