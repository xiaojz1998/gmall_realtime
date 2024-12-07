package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;

/**
 * @author Felix
 * @date 2024/12/03
 * 获取FlinkSQL相关连接器属性的工具类
 */
public class SQLUtil {
    //获取kafka连接器相关连接属性
    public static String getKafkaDDL(String topic, String groupId) {
        return "   WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    //获取hbase连接器相关连接属性
    public static String getHBaseDDL(String tableName) {
        return " WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '" + Constant.HBASE_NAMESPACE + ":" + tableName + "',\n" +
                " 'zookeeper.quorum' = 'hadoop102,hadoop103,hadoop104:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '200',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }

    //获取upsert-kafka连接器相关属性
    public static String getUpsertKafkaDDL(String topic) {
        return " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

    //获取Doris连接器的连接属性
    public static String getDorisDDL(String tableName){
        return  "       WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '"+Constant.DORIS_FE_NODES+"',\n" +
                "      'table.identifier' = '"+Constant.DORIS_DATABASE+"."+tableName+"',\n" +
                "      'username' = 'root',\n" +
                "      'password' = 'aaaaaa',\n" +
                "      'sink.enable-2pc' = 'false'\n" +
                ")";
    }
}
