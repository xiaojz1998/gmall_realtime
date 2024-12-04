package com.atguigu.gmall.realtime.dwd.db.split.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/**
 * @author Felix
 * @date 2024/12/04
 * 对关联后的数据进行处理
 */
public class BaseDbTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject,TableProcessDwd>> {
    private MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;
    private Map<String,TableProcessDwd> configMap = new HashMap<>();

    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    //将配置表的配置信息预加载到configMap
    @Override
    public void open(Configuration parameters) throws Exception {
        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        String sql = "select * from gmall0620_config.table_process_dwd";
        List<TableProcessDwd> tableProcessDwdList = JdbcUtil.queryList(mysqlConnection, sql, TableProcessDwd.class, true);
        for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
            String sourceTable = tableProcessDwd.getSourceTable();
            String sourceType = tableProcessDwd.getSourceType();
            String key = getKey(sourceTable, sourceType);
            configMap.put(key,tableProcessDwd);
        }
        JdbcUtil.closeMysqlConnection(mysqlConnection);
    }

    public String getKey(String sourceTable, String sourceType) {
        String key = sourceTable + ":" + sourceType;
        return key;
    }

    //对主流数据进行处理
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        //获取表名
        String table = jsonObj.getString("table");
        //获取对业务数据库表进行操作的类型
        String type = jsonObj.getString("type");
        //拼接key
        String key = getKey(table, type);
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //根据key到广播状态以及configMap中获取对应的配置
        TableProcessDwd tableProcessDwd = null;
        if((tableProcessDwd = broadcastState.get(key)) != null
           ||(tableProcessDwd = configMap.get(key)) != null){
            //如果配置对象不为空，将其中data部分内容以及对应的配置对象 封装为二元组向下游传递
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            //过滤掉不需要传递的字段
            String sinkColumns = tableProcessDwd.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj,sinkColumns);
            //在向下游传递数据前，补充ts字段
            dataJsonObj.put("ts",jsonObj.getLong("ts"));
            out.collect(Tuple2.of(dataJsonObj,tableProcessDwd));
        }

    }

    private void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry->!columnList.contains(entry.getKey()));
    }

    //对广播流数据进行处理
    @Override
    public void processBroadcastElement(TableProcessDwd tp, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        //获取对配置表进行的操作的类型
        String op = tp.getOp();
        //获取广播状态
        BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String sourceTable = tp.getSourceTable();
        String sourceType = tp.getSourceType();
        String key = getKey(sourceTable, sourceType);
        if("d".equals(op)){
            //说明从配置表中删除了一条配置信息，需要从广播状态以及configMap中删除对应的配置
            broadcastState.remove(key);
            configMap.remove(key);
        }else{
            //说明对配置表进行了读取、添加、更新操作，需要将最新的配置put到广播状态以及configMap中
            broadcastState.put(key,tp);
            configMap.put(key,tp);
        }
    }
}
