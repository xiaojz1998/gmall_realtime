package com.atguigu.gmall.realtime.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author Felix
 * @date 2024/11/29
 * 操作HBase的工具类
 */
public class HBaseUtil {
    //获取连接
    public static Connection getHBaseConnection(){
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            Connection hbaseConn = ConnectionFactory.createConnection(conf);
            return hbaseConn;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //关闭连接
    public static void closeHBaseConnection(Connection hbaseConn){
        if(hbaseConn != null && !hbaseConn.isClosed()){
            try {
                hbaseConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    //建表
    //删表

}
