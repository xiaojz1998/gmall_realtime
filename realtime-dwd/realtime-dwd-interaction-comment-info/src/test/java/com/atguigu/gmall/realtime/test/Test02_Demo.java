package com.atguigu.gmall.realtime.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2024/12/02
 * 该案例模拟评论事实表的实现过程
 */
public class Test02_Demo {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.从kafka的主题中读取员工数据
        tableEnv.executeSql("CREATE TABLE emp (\n" +
                "  empno string, \n" +
                "  ename string,\n" +
                "  deptno string,\n" +
                "  proc_time AS PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        //tableEnv.executeSql("select * from emp").print();

        //TODO 3.从HBase表中读取部门数据
        tableEnv.executeSql("CREATE TABLE dept (\n" +
                " deptno string,\n" +
                " info ROW<dname string>,\n" +
                " PRIMARY KEY (deptno) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 't_dept',\n" +
                " 'zookeeper.quorum' = 'hadoop102,hadoop103,hadoop104:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '200',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")");
        //tableEnv.executeSql("select deptno,dname from dept").print();

        //TODO 4.将员工和部门进行关联
        //注意：这里不能使用普通的内外连接，因为状态的失效时间没有办法设置
        //我们这里使用LookupJoin，LookupJoin底层实现原理和普通的内外连接完全不一样，不会为参与连接的两张表维护状态
        //它是以左表进行驱动，当左表数据到来的时候，发送请求和右表进行关联
        Table joinedTable = tableEnv.sqlQuery("SELECT e.empno,e.ename,d.deptno,d.dname\n" +
                "FROM emp AS e\n" +
                "  JOIN dept FOR SYSTEM_TIME AS OF e.proc_time AS d\n" +
                "    ON e.deptno=d.deptno");

        //joinedTable.execute().print();

        //TODO 5.将关联的结果写到kafka主题
        //5.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE joined_table (\n" +
                "  empno string,\n" +
                "  ename  string,\n" +
                "  deptno  string,\n" +
                "  dname  string,\n" +
                "  PRIMARY KEY (empno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'second',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");
        //5.2 写入
        joinedTable.executeInsert("joined_table");
    }
}
