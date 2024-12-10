package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.DimJoinFunction;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Felix
 * @date 2024/12/10
 * 发送异步请求进行维度关联
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {
    StatefulRedisConnection<String, String> redisAsyncConn;
    AsyncConnection hBaseAsyncConn;
    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
        HBaseUtil.closeHBaseAsyncConnection(hBaseAsyncConn);
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //创建异步编排对象   ---有返回值    这个返回值将作为下一个线程任务的入参
        CompletableFuture.supplyAsync(
                new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        //先以异步的方式从Redis中获取维度数据
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, getTableName(), getRowKey(obj));
                        return dimJsonObj;
                    }
                }
        ).thenApplyAsync(
                //创建线程任务    有入参  有返回值
                new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimJsonObj) {
                        if(dimJsonObj != null){
                            //如果从Redis中获取到了要关联的维度，直接将其进行返回(缓存命中)
                            System.out.println("~~~从Redis中获取到了"+getTableName()+"表的"+getRowKey(obj)+"数据~~~");
                        }else {
                            //如果从Redis中没有获取到了要关联的维度，发送异步请求到HBase中查询维度
                            dimJsonObj = HBaseUtil.readDimAsync(hBaseAsyncConn, Constant.HBASE_NAMESPACE,getTableName(),getRowKey(obj));
                            if(dimJsonObj != null){
                                //以异步的方式将查询的结果放到Redis中缓存起来
                                System.out.println("~~~从HBase中获取到了"+getTableName()+"表的"+getRowKey(obj)+"数据~~~");
                                RedisUtil.writeDimAsync(redisAsyncConn,getTableName(),getRowKey(obj),dimJsonObj);
                            }else{
                                System.out.println("~~~没有找到"+getTableName()+"表的"+getRowKey(obj)+"数据~~~");
                            }
                        }
                        return dimJsonObj;
                    }
                }
        ).thenAcceptAsync(
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dimJsonObj) {
                        if(dimJsonObj != null){
                            //补充维度属性到流中对象上
                            addDims(obj,dimJsonObj);
                        }
                        //向下游传递数据
                        resultFuture.complete(Collections.singleton(obj));
                    }
                }
        );
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        throw new RuntimeException("1:检查维度表历史数据是否同步\n" +
                "2:检查zk、kafka、maxwell、hdfs、hbase、redis是否正常启动\n" +
                "3:找昌平彦祖\n");
    }
}
