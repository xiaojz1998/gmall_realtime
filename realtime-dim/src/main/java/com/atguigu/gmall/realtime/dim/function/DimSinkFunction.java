package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * @author Felix
 * @date 2024/11/30
 * 将流中数据同步到HBase
 */
public class DimSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hBaseConn;
    private Jedis jedis;
    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseConn = HBaseUtil.getHBaseConnection();
        jedis = RedisUtil.getJedis();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hBaseConn);
        RedisUtil.closeJedis(jedis);
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
            HBaseUtil.delRow(hBaseConn, Constant.HBASE_NAMESPACE,sinkTable,rowKey);
        }else {
            //insert、update、bootstrap-insert     对HBase进行put操作
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(hBaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey,sinkFamily,dataJsonObj);
        }

        //如果维度数据发生了变化，从Redis中将缓存的数据清除掉
        if("delete".equals(type)||"update".equals(type)){
            jedis.del(RedisUtil.getKey(sinkTable,rowKey));
        }

    }
}
