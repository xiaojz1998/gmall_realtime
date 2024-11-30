package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.dim.function.DimSinkFunction;
import com.atguigu.gmall.realtime.dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @author Felix
 * @date 2024/11/29
 * Dim层开发
 * 需要启动的进程
 *      zk、kafka、maxwell、hdfs、hbase、DimApp
 * 开发流程
 *      基本环境准备
 *      检查点相关的设置
 *      从kafka主题中读取数据
 *      对流中数据进行类型转换并进行ETL     jsonStr->jsonObj
 *      ~~~~~~~~~~~~~~~~~~~~~主流~~~~~~~~~~~~~~~~~~~~~~~~~~
 *      使用FlinkCDC读取配置表中的配置
 *      根据配置表中的配置在HBase中建表或者删除表
 *      ~~~~~~~~~~~~~~~~~~~~~配置流~~~~~~~~~~~~~~~~~~~~~~~~~~
 *      广播配置流---broadcast
 *      将主流业务数据和广播流配置数据进行关联---connect
 *      对关联后的数据进行处理---process
 *          new BroadcastProcessFunction{
 *              open: 将配置表中的配置预加载到configMap中
 *              processElement:对主流数据进行处理
 *                  根据表名到广播状态以及configMap中获取对应的配置配置信息
 *                  如果能够获取到对应的配置，说明是维度数据，将其封装为Tuple2向下游传递
 *                  在向下游传递数据前，过滤掉不需要传递的字段
 *                  在向下游传递数据前，补充type字段
 *              processBroadcastElement:对广播流数据进行处理
 *                  op=d
 *                      从广播状态以及configMap中删除对应的配置
 *                  op！=d
 *                      将最新的配置信息放到广播状态以及configMap
 *          }
 *      将维度数据同步到HBase表中
 *          new RichSickFunction{
 *              invoke:
 *                  type=delete
 *                      从Hbase表中删除对应的记录
 *                  type!= delete
 *                      向HBase表中put数据
 *          }
 * 执行流程（以对业务数据库品牌维度表中的一条数据进行修改为例）
 *      当DimApp程序启动的时候，会将配置表的配置信息加载到configMap以及广播状态中
 *      如果修改了业务数据品牌维度表数据
 *      binlog会记录变化
 *      maxwell会将变化捕获到并封装为jsonStr发送到kafka的topic_db主题中
 *      DimApp会从topic_db主题中读取数据（主流）
 *      根据表名判断是否为维度，如果是维度，同步到HBase中
 */
public class DimApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimApp().start(10002,4,"dim_app",Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.对流中数据进行类型转换并进行简单的ETL    jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);
        //TODO 2.使用FlinkCDC读取配置表中的配置信息
        SingleOutputStreamOperator<TableProcessDim> tpDS = readTableProcess(env);
        //TODO 3.根据配置表中的配置信息在HBase中执行建表或者删表操作
        tpDS = createHBaseTable(tpDS);
        //TODO 4.广播配置流---broadcast  将主流和广播流进行关联---connect   对关联后的数据进行处理
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connect(tpDS, jsonObjDS);
        //TODO 5.将维度数据同步到HBase
        write2Hbase(dimDS);
    }

    private static void write2Hbase(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS) {
        dimDS.print();
        dimDS.addSink(new DimSinkFunction());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connect(SingleOutputStreamOperator<TableProcessDim> tpDS, SingleOutputStreamOperator<JSONObject> jsonObjDS) {
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);


        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        return dimDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHBaseTable(SingleOutputStreamOperator<TableProcessDim> tpDS) {
        tpDS = tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {
                    private Connection hBaseConn;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hBaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hBaseConn);
                    }
                    @Override
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        //获取对配置表进行操作的类型
                        String op = tp.getOp();
                        String sinkTable = tp.getSinkTable();
                        String[] families = tp.getSinkFamily().split(",");
                        if("r".equals(op)||"c".equals(op)){
                            //从配置表中读取一条数据或者向配置表中添加一条配置  执行建表操作
                            HBaseUtil.createHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable,families);
                        }else if ("d".equals(op)){
                            //从配置表中删除了一条配置  执行删表操作
                            HBaseUtil.dropHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                        }else{
                            //对配置表中的某条数据进行了更新操作  先删表再建表
                            HBaseUtil.dropHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable,families);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);
        //tpDS.print();
        return tpDS;
    }

    private static SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        //2.1 创建MySqlSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("table_process_dim");
        //2.2 读取数据封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
        //"op":"r" {"before":null,"after":{"source_table":"financial_sku_cost","sink_table":"dim_financial_sku_cost","sink_family":"info","sink_columns":"id,sku_id,sku_name,busi_date,is_lastest,sku_cost,create_time","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1732870578960,"transaction":null}
        //"op":"c" {"before":null,"after":{"source_table":"t_a","sink_table":"dim_a","sink_family":"info","sink_columns":"id,name","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732870643000,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11499579,"row":0,"thread":14,"query":null},"op":"c","ts_ms":1732870642929,"transaction":null}
        //"op":"u" {"before":{"source_table":"t_a","sink_table":"dim_a","sink_family":"info","sink_columns":"id,name","sink_row_key":"id"},"after":{"source_table":"t_a","sink_table":"dim_a","sink_family":"info","sink_columns":"id,name,age","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732870709000,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11499941,"row":0,"thread":14,"query":null},"op":"u","ts_ms":1732870709291,"transaction":null}
        //"op":"d" {"before":{"source_table":"t_a","sink_table":"dim_a","sink_family":"info","sink_columns":"id,name,age","sink_row_key":"id"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732870748000,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11500331,"row":0,"thread":14,"query":null},"op":"d","ts_ms":1732870748451,"transaction":null}
        //mysqlStrDS.print();

        //2.3 对流中数据进行类型转换   jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String jsonStr) throws Exception {
                //为了处理方便，将jsonStr转换为jsonObj
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                //获取对配置表进行的操作的类型
                String op = jsonObj.getString("op");
                TableProcessDim tableProcessDim = null;
                if ("d".equals(op)) {
                    //对配置表进行了删除操作   从before属性中获取删除前的信息
                    tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                } else {
                    //对配置表进行了读取、添加、更新操作  从after属性中获取最新的配置信息
                    tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                }
                //补充操作类型
                tableProcessDim.setOp(op);
                return tableProcessDim;
            }
        }).setParallelism(1);

        //tpDS.print();
        return tpDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String db = jsonObj.getString("database");
                            String type = jsonObj.getString("type");
                            String data = jsonObj.getString("data");
                            if ("gmall0620".equals(db)
                                    && ("insert".equals(type)
                                    || "update".equals(type)
                                    || "delete".equals(type)
                                    || "bootstrap-insert".equals(type))
                                    && data != null
                                    && data.length() > 2
                            ) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            System.out.println("不是一个标准的json");
                        }
                    }
                }
        );
        //jsonObjDS.print();
        return jsonObjDS;
    }
}