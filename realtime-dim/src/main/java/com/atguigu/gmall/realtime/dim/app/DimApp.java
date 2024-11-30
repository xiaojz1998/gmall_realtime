package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @author Felix
 * @date 2024/11/29
 * Dim层开发
 * 需要启动的进程
 *      zk、kafka、maxwell、hdfs、hbase、DimApp
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8088);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //2.2 设置检查点超时时间
        checkpointConfig.setCheckpointTimeout(60000L);
        //2.3 设置job取消之后检查点是否保留
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端
        /*
                                        状态                  检查点
            hashmap                   TM堆内存                JM堆内存
            rocksDB                   RocksDB库              文件系统
        */
        //env.setStateBackend(new HashMapStateBackend());
        //2.7 设置检查点存储路径
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck");
        //2.8 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //TODO 3.从kafka主题中读取主流业务数据
        //3.1 声明消费的主题以及消费者组
        String groupId = "dim_app";
        //3.2 创建消费者对象
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_DB)
                .setGroupId(groupId)
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setStartingOffsets(OffsetsInitializer.latest())
                //注意：如果使用Flink提供的针对字符串进行反序列化的SimpleStringSchema，不能处理从kafka读取到的空消息
                //如果要向处理空消息，需要自定义反序列化的实现
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if(message != null){
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        //TODO 4.对流中数据进行类型转换并进行简单的ETL    jsonStr->jsonObj
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

        //TODO 5.使用FlinkCDC读取配置表中的配置信息
        //5.1 创建MySqlSource对象
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList("gmall0620_config")
                .tableList("gmall0620_config.table_process_dim")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .jdbcProperties(props)
                .build();
        //5.2 读取数据封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
        //"op":"r" {"before":null,"after":{"source_table":"financial_sku_cost","sink_table":"dim_financial_sku_cost","sink_family":"info","sink_columns":"id,sku_id,sku_name,busi_date,is_lastest,sku_cost,create_time","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1732870578960,"transaction":null}
        //"op":"c" {"before":null,"after":{"source_table":"t_a","sink_table":"dim_a","sink_family":"info","sink_columns":"id,name","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732870643000,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11499579,"row":0,"thread":14,"query":null},"op":"c","ts_ms":1732870642929,"transaction":null}
        //"op":"u" {"before":{"source_table":"t_a","sink_table":"dim_a","sink_family":"info","sink_columns":"id,name","sink_row_key":"id"},"after":{"source_table":"t_a","sink_table":"dim_a","sink_family":"info","sink_columns":"id,name,age","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732870709000,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11499941,"row":0,"thread":14,"query":null},"op":"u","ts_ms":1732870709291,"transaction":null}
        //"op":"d" {"before":{"source_table":"t_a","sink_table":"dim_a","sink_family":"info","sink_columns":"id,name,age","sink_row_key":"id"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732870748000,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11500331,"row":0,"thread":14,"query":null},"op":"d","ts_ms":1732870748451,"transaction":null}
        //mysqlStrDS.print();

        //5.3 对流中数据进行类型转换   jsonStr->实体类对象
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

        //TODO 6.根据配置表中的配置信息在HBase中执行建表或者删表操作
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


        //TODO 7.广播配置流---broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //TODO 8.将主流和广播流进行关联---connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        //TODO 9.对关联后的数据进行处理
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {
            private Map<String,TableProcessDim> configMap = new HashMap<>();
            @Override
            public void open(Configuration parameters) throws Exception {
                //将配置表中的配置信息预加载到configMap
                //注册驱动
                Class.forName("com.mysql.cj.jdbc.Driver");
                //建立连接
                java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
                //获取数据库操作对象
                String sql = "select * from gmall0620_config.table_process_dim";
                PreparedStatement ps = conn.prepareStatement(sql);
                //执行SQL语句
                ResultSet rs = ps.executeQuery();
                ResultSetMetaData metaData = rs.getMetaData();
                //处理结果集
                while (rs.next()){
                    //定义一个json对象，用于接收查询出来的一条结果
                    JSONObject jsonObj = new JSONObject();
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i);
                        Object columnValue = rs.getObject(i);
                        jsonObj.put(columnName,columnValue);
                    }
                    //将jsonObj转换为实体类对象
                    TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
                    configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
                }

                //释放资源
                rs.close();
                ps.close();
                conn.close();
            }

            //处理主流业务数据
            @Override
            public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                //{"database":"gmall0620","data":{"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"aaa","id":1},"type":"update","table":"base_trademark","ts":1732932983}
                //System.out.println(jsonObj);
                //获取当前操作的业务数据库表表名
                String table = jsonObj.getString("table");
                //获取广播状态
                ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                //根据表名到广播状态中获取对应的配置信息
                TableProcessDim tableProcessDim = null;

                if((tableProcessDim = broadcastState.get(table)) != null
                    ||(tableProcessDim = configMap.get(table)) != null){
                    //说明当前处理的这条数据 是维度数据  需要将这条维度数据data部分内容以及对应的配置对象封装为二元组向下游传递
                    //{"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"aaabbbccc","id":1}
                    JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                    //在向下游传递数据前  过滤掉不需要传递的字段
                    String sinkColumns = tableProcessDim.getSinkColumns();
                    deleteNotNeedColumns(dataJsonObj,sinkColumns);
                    //在向下游传递数据前  需要补充操作类型字段
                    //{"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"aaabbbccc","id":1,"type":"update"}
                    String type = jsonObj.getString("type");
                    dataJsonObj.put("type",type);
                    out.collect(Tuple2.of(dataJsonObj,tableProcessDim));
                }
            }

            //处理广播流配置信息
            @Override
            public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                //获取对配置表进行操作的类型
                String op = tp.getOp();
                //获取广播状态
                BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                String sourceTable = tp.getSourceTable();
                if("d".equals(op)){
                    //说明从配置表中删除了一条配置信息  从广播状态中将对应的配置删除
                    broadcastState.remove(sourceTable);
                    configMap.remove(sourceTable);
                }else {
                    //说明对配置表进行了读取、添加或者修改操作   将最新的配置信息放到广播状态中
                    broadcastState.put(sourceTable,tp);
                    configMap.put(sourceTable,tp);
                }
            }
        });
        //TODO 10.将维度数据同步到HBase
        dimDS.print();
        dimDS.addSink(new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {
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
            public void invoke(Tuple2<JSONObject, TableProcessDim> tup2, Context context) throws Exception {
                JSONObject dataJsonObj = tup2.f0;
                TableProcessDim tableProcessDim = tup2.f1;
                //{"id":"1","tm_name":"redmi","type":"insert"}
                String type = dataJsonObj.getString("type");
                dataJsonObj.remove("type");

                String sinkTable = tableProcessDim.getSinkTable();
                //注意：获取的是rowkey的值
                String rowKey = dataJsonObj.getString(tableProcessDim.getSinkRowKey());
                if("delete".equals(type)){
                    //说明从业务数据库的维度表中删除了一条数据    从Hbase表中也少删除这条数据
                    HBaseUtil.delRow(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey);
                }else {
                    //insert、update、bootstrap-insert     对HBase进行put操作
                    String sinkFamily = tableProcessDim.getSinkFamily();
                    HBaseUtil.putRow(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey,sinkFamily,dataJsonObj);
                }
            }
        });


        env.execute();
    }

    //过滤掉不需要保留的字段
    //dataJsonObj: {"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"aaabbbccc","id":1}
    //sinkColumns: id,tm_name
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));
    }
}