package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Set;

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
    public static void createHBaseTable(Connection hbaseConn,String namespace,String tableName,String ... families){
        if(families.length < 1){
            System.out.println("~~~创建表至少需要传递一个列族~~~");
            return;
        }
        try (Admin admin = hbaseConn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if(admin.tableExists(tableNameObj)){
                System.out.println("~~~要创建"+namespace+"表空间下的表"+tableName+"已存在~~~");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build());
            }

            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("~~~创建"+namespace+"表空间下的表"+tableName+"成功~~~");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //删表
    public static void dropHBaseTable(Connection hbaseConn,String namespace,String tableName){
        try (Admin admin = hbaseConn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if(!admin.tableExists(tableNameObj)){
                System.out.println("~~~要删除"+namespace+"表空间下的表"+tableName+"不存在~~~");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("~~~删除"+namespace+"表空间下的表"+tableName+"成功~~~");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //从HBase表中删除数据
    public static void delRow(Connection hbaseConn,String namespace,String tableName,String rowkey){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            table.delete(delete);
            System.out.println("~~~删除"+namespace+"表空间下的表"+tableName+"的数据"+rowkey+"成功~~~");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //向HBase表中put数据
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowkey, String family, JSONObject jsonObj){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Put put = new Put(Bytes.toBytes(rowkey));
            Set<String> columnNames = jsonObj.keySet();
            for (String columnName : columnNames) {
                String columnValue = jsonObj.getString(columnName);
                if(StringUtils.isNotEmpty(columnValue)){
                    put.addColumn(Bytes.toBytes(family),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));
                }
            }
            table.put(put);
            System.out.println("~~~向"+namespace+"表空间下的表"+tableName+"put数据"+rowkey+"成功~~~");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
