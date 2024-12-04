package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.dwd.db.split.function.BaseDbTableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/12/04
 * 事实表动态分流处理
 * 需要启动的进程
 *      zk、kafka、maxwell、DwdBaseDb
 */
public class DwdBaseDb  extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(10019,
                4,
                "dwd_base_db",
                Constant.TOPIC_DB
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.对流中数据进行类型转换并ETL    jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String type = jsonObj.getString("type");
                            if (!type.startsWith("bootstrap-")) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            System.out.println("~~~不是一个标准的json~~~");
                        }
                    }
                }
        );
        //jsonObjDS.print();
        //TODO 2.使用FlinkCDC读取配置表中的配置信息
        //2.1 创建MySqlSource
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("table_process_dwd");
        //2.2 读取数据封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
        //2.3 对流中数据进行类型转换   jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String jsonStr) throws Exception {
                        //为了处理方便先将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //获取对配置表进行的操作类型
                        String op = jsonObj.getString("op");
                        TableProcessDwd tableProcessDwd = null;
                        if ("d".equals(op)) {
                            //说明从配置表删除了一条配置信息，从before属性中获取删除前的配置
                            tableProcessDwd = jsonObj.getObject("before", TableProcessDwd.class);
                        } else {
                            //说明对配置表进行了读取、添加、修改操作，从after属性中获取最新的配置信息
                            tableProcessDwd = jsonObj.getObject("after", TableProcessDwd.class);
                        }
                        //补充操作类型到对象上
                        tableProcessDwd.setOp(op);
                        return tableProcessDwd;
                    }
                }
        ).setParallelism(1);
        //tpDS.print();

        //TODO 3.广播配置流---broadcast
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDwd>("mapStateDescriptor", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //TODO 4.将主流业务数据和广播流配置信息进行关联---connect
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);

        //TODO 5.对关联后的数据进行处理---process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> realDS = connectDS.process(
                new BaseDbTableProcessFunction(mapStateDescriptor)
        );
        //TODO 6.将流中数据写到kafka的不同主题中
        realDS.print();
        realDS.sinkTo(FlinkSinkUtil.getKafkaSink());
    }
}
