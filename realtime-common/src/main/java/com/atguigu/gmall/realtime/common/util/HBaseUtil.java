package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @author Felix
 * @date 2024/11/29
 * 操作HBase的工具类
 */
public class HBaseUtil {
    //获取异步连接
    public static AsyncConnection getHBaseAsyncConnection(){
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            AsyncConnection hbaseAsyncConn = ConnectionFactory.createAsyncConnection(conf).get();
            return hbaseAsyncConn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    //关闭异步连接
    public static void closeHBaseAsyncConnection(AsyncConnection hbaseAsyncConn){
        if(hbaseAsyncConn != null && !hbaseAsyncConn.isClosed()){
            try {
                hbaseAsyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
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

    /**
     * 根据rowkey从HBase表中读取一条数据
     * @param hbaseConn                 连接对象
     * @param namespace                 表空间
     * @param tableName                 表名
     * @param rowkey                    rowkey
     * @param clz                       要封装的对象类型
     * @param isUnderlineToCamel        是否要将下划线转换为驼峰命名法
     * @return
     * @param <T>
     */
    public static <T>T getRow(Connection hbaseConn, String namespace, String tableName, String rowkey,Class<T> clz,boolean... isUnderlineToCamel){
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            if(cells != null &&cells.size() > 0){
                //定义一个对象，用于封装查询出来的这一行数据
                T obj = clz.newInstance();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if(defaultIsUToC){
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }
                    BeanUtils.setProperty(obj,columnName,columnValue);
                }
                return obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    //以异步的方式从HBase表中读取数据
    public static JSONObject readDimAsync(AsyncConnection hbaseAsyncConn,String namespace,String tableName,String rowkey){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        AsyncTable<AdvancedScanResultConsumer> asyncTable = hbaseAsyncConn.getTable(tableNameObj);
        Get get = new Get(Bytes.toBytes(rowkey));
        try {
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if(cells != null && cells.size() > 0){
                JSONObject dimJsonObj = new JSONObject();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    dimJsonObj.put(columnName,columnValue);
                }
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static void main(String[] args) {
        Connection hBaseConn = getHBaseConnection();
        System.out.println(getRow(hBaseConn, Constant.HBASE_NAMESPACE, "dim_base_trademark", "1", JSONObject.class));
        closeHBaseConnection(hBaseConn);
    }
}
