package com.atguigu.gmall.realtime.test;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2024/12/06
 * 该案例演示了通过FlinkSQL读写Doris表中数据
 */
public class Test01_Flink_Doris_SQL {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.检查点相关的设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //TODO 3.读数据
    /*    tableEnv.executeSql("CREATE TABLE flink_doris (  " +
                "    siteid INT,  " +
                "    citycode SMALLINT,  " +
                "    username STRING,  " +
                "    pv BIGINT  " +
                "    )   " +
                "    WITH (  " +
                "      'connector' = 'doris',  " +
                "      'fenodes' = 'hadoop102:7030',  " +
                "      'table.identifier' = 'test.table1',  " +
                "      'username' = 'root',  " +
                "      'password' = 'aaaaaa'  " +
                ")  ");*/
        // 读
        //tableEnv.sqlQuery("select * from flink_doris").execute().print();

        //TODO 4.写数据
        tableEnv.executeSql("CREATE TABLE flink_doris (  " +
                "    siteid INT,  " +
                "    citycode INT,  " +
                "    username STRING,  " +
                "    pv BIGINT  " +
                ")WITH (" +
                "  'connector' = 'doris', " +
                "  'fenodes' = 'hadoop102:7030', " +
                "  'table.identifier' = 'test.table1', " +
                "  'username' = 'root', " +
                "  'password' = 'aaaaaa', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false' " + // 测试阶段可以关闭两阶段提交,方便测试
                ")  ");


        tableEnv.executeSql("insert into flink_doris values(33, 3, '深圳', 3333)");

    }
}
