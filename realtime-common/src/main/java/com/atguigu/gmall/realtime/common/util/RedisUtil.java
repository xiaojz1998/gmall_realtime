package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Felix
 * @date 2024/12/10
 * 操作Redis的工具类
 * 旁路缓存
 *      思路：先从缓存中获取对应的维度数据，如果在缓存中，找到了对应的维度，直接将其进行返回(缓存命中);
 *           如果从缓存中没有找到要关联的维度，发送请求到HBase中查找，并将查询到的数据放到Redis中缓存起来，方便下次查询使用
 *      选型
 *          redis         性能不错，维护性好     √
 *          状态          性能很好，维护性差
 *      关于Redis的配置
 *          key：    维度表名:主键值        例如：     dim_base_trademark:1
 *          type:   string
 *          expire； 1day    避免冷数据常驻内存，给内存带来压力
 *          注意：如果维度数据发生了变化，需要将缓存的数据清除掉
 */
public class RedisUtil {
    private static JedisPool jedisPool;
    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",6379,10000);
    }
    //获取Jedis客户端
    public static Jedis getJedis(){
        System.out.println("~~~获取Jedis客户端~~~");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }
    //关闭Jedis客户端
    public static void closeJedis(Jedis jedis){
        System.out.println("~~~关闭Jedis客户端~~~");
        if(jedis != null){
            jedis.close();
        }
    }

    //从Redis中读取数据
    public static JSONObject readDim(Jedis jedis,String tableName,String id){
        //拼接key
        String key = getKey(tableName, id);
        //根据key到Redis中获取维度数据
        String dimJsonStr = jedis.get(key);
        if(StringUtils.isNotEmpty(dimJsonStr)){
            JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
            return dimJsonObj;
        }
        return null;
    }
    //向Redis中放数据
    public static void writeDim(Jedis jedis,String tableName,String id,JSONObject dimJsonObj){
        String key = getKey(tableName, id);
        jedis.setex(key,24*3600,dimJsonObj.toJSONString());
    }
    public static String getKey(String tableName, String id) {
        String key = tableName + ":" + id;
        return key;
    }



    public static void main(String[] args) {
        Jedis jedis = getJedis();
        System.out.println(jedis.ping());
        closeJedis(jedis);
    }
}
