package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
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
 * @date 2024/11/30
 * 对关联后的数据进行处理(过滤出维度数据)
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    private Map<String,TableProcessDim> configMap = new HashMap<>();
    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        String sql = "select * from gmall0620_config.table_process_dim";
        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mysqlConnection, sql, TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeMysqlConnection(mysqlConnection);
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

    //过滤掉不需要保留的字段
    //dataJsonObj: {"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"aaabbbccc","id":1}
    //sinkColumns: id,tm_name
    private void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));
    }
}
