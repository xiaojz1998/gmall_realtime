package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.DimJoinFunction;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * @author Felix
 * @date 2024/12/10
 * 使用旁路缓存进行维度关联的模板类
 */
public abstract class DimMapFunction<T> extends RichMapFunction<T, T> implements DimJoinFunction<T> {
    Connection hBaseConn;
    Jedis jedis;

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
    public T map(T obj) throws Exception {
        //根据流中对象获取要关联的维度的主键
        String key = getRowKey(obj);
        //先从Redis中获取维度数据
        JSONObject dimJsonObj = RedisUtil.readDim(jedis, getTableName(), key);
        if (dimJsonObj != null) {
            //如果从Redis中获取到了要关联的维度，直接将其进行返回(缓存命中)
            System.out.println("~~~从Redis中获取到" + getTableName() + "表的" + key + "数据~~~");
        } else {
            //如果从Redis中没有获取到了要关联的维度，发送请求到HBase中查询维度
            dimJsonObj = HBaseUtil.getRow(hBaseConn, Constant.HBASE_NAMESPACE,getTableName(),key, JSONObject.class);
            if(dimJsonObj != null){
                System.out.println("~~~从HBase中获取到" + getTableName() + "表的" + key + "数据~~~");
                //将查询的结果放到Redis中缓存起来
                RedisUtil.writeDim(jedis,getTableName(),key,dimJsonObj);
            }else{
                System.out.println("~~~没有获取到" + getTableName() + "表的" + key + "数据~~~");
            }
        }
        if(dimJsonObj != null){
            //补充维度属性到流中对象上
            addDims(obj,dimJsonObj);
        }

        return obj;
    }
}
