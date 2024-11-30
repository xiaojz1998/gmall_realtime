package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Felix
 * @date 2024/11/30
 * 通用的操作Mysql的工具类
 */
public class JdbcUtil {
    //获取mysql连接
    public static Connection getMysqlConnection() throws Exception {
        //注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        //建立连接
        Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
        return conn;
    }
    //关闭mysql连接
    public static void closeMysqlConnection(Connection conn) throws Exception{
        if(conn != null && !conn.isClosed()){
            conn.close();
        }
    }
    //通用从Mysql表中查询数据的方法
    public static <T>List<T> queryList(Connection conn,String sql,Class<T> clz)throws Exception{
        return queryList(conn,sql,clz,false);
    }
    public static <T>List<T> queryList(Connection conn,String sql,Class<T> clz,boolean isUnderLineToCamel)throws Exception{
        List<T> resList = new ArrayList<>();
        //获取数据库操作对象
        PreparedStatement ps = conn.prepareStatement(sql);
        //执行SQL
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        //处理结果集
        while (rs.next()){
            //定义一个实例用于接收查询出来的一行数据
            T obj = clz.newInstance();
            //给对象的属性赋值
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);

                if(isUnderLineToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                }
                BeanUtils.setProperty(obj,columnName,columnValue);
            }

            resList.add(obj);
        }

        //释放资源
        rs.close();
        ps.close();
        return resList;
    }
}
