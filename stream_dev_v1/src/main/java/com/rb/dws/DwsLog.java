package com.rb.dws;

import com.rb.dws.uitil.IkTest;
import com.rb.dws.uitil.UdtfTest;
import com.rb.utils.SQLUtil;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * @Package com.rb.dws.DwsLog
 * @Author runbo.zhang
 * @Date 2025/4/14 19:06
 * @description:
 */
public class DwsLog {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/flink/checkpoints");
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //TODO 从页面日志事实表中读取数据 创建动态表  并指定Watermark的生成策略以及提取事件时间字段
        tEnv.executeSql("create table page_log(\n" +
                "     common map<string,string>,\n" +
                "     page map<string,string>,\n" +
                "     ts bigint,\n" +
                "     et as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "     WATERMARK FOR et AS et\n" +
                ")" + SQLUtil.getKafkaDDL("log_topic_flink_online_v2_log_page"));
//        tEnv.executeSql("select * from page_log").print();

        Table searchTable = tEnv.sqlQuery("select " +
                " `page`['item'] as  fullwords,\n" +
                " `et` \n" +
                " from page_log \n" +
                " where `page`['last_page_id']='search' and `page`['item'] is not null and `page`['item_type'] ='keyword'"
        );
        tEnv.createTemporaryView("search_table", searchTable);

        tEnv.createTemporaryFunction("ik_split", UdtfTest.class);
        Table keyWordTable = tEnv.sqlQuery(
                "SELECT keyword, et " +
                        "FROM search_table, LATERAL TABLE(ik_split(fullwords)) T(keyword)");
        tEnv.createTemporaryView("split_table", keyWordTable);
        Table resTable = tEnv.sqlQuery("SELECT \n" +
                "     date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "     date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "     date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "     keyword,\n" +
                "     count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' second))\n" +
                "  GROUP BY window_start, window_end,keyword");

        resTable.execute().print();
//        table.execute().print();

    }
}
