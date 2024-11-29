package com.atguigu.gmall.realtime.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2024/11/29
 * 该案例演示了FlinkCDC的使用
 */
public class Test01_FlinkCDC {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);

        //TODO 2.检查点相关的设置
        env.enableCheckpointing(3000);

        //TODO 3.使用FlinkCDC读取数据
        //3.1 创建MySqlSource对象
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall0620_config")
                .tableList("gmall0620_config.t_user")
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        //3.2 读取数据 封装为流
        //TODO 4.打印输出
        //"op":"r" {"before":null,"after":{"id":2,"name":"ls","age":30},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"t_user","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1732863127760,"transaction":null}
        //"op":"c" {"before":null,"after":{"id":3,"name":"ww","age":40},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732863349000,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":394,"row":0,"thread":14,"query":null},"op":"c","ts_ms":1732863349529,"transaction":null}
        //"op":"u" {"before":{"id":3,"name":"ww","age":40},"after":{"id":3,"name":"ww","age":18},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732863398000,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":719,"row":0,"thread":14,"query":null},"op":"u","ts_ms":1732863398137,"transaction":null}
        //"op":"d" {"before":{"id":3,"name":"ww","age":18},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732863428000,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":1049,"row":0,"thread":14,"query":null},"op":"d","ts_ms":1732863428319,"transaction":null}



        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print();
        //TODO 5.提交作业
        try {
            env.execute("Print MySQL Snapshot + Binlog");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
