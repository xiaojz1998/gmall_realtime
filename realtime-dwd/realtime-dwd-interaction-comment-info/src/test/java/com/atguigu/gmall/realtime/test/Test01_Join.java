package com.atguigu.gmall.realtime.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author Felix
 * @date 2024/12/02
 * 该案例演示了FlinkSQL中的join
 * 在FlinkAPI中合流算子
 *      connect
 *      union
 * 在FlinkAPI对指定时间范围的数据进行join
 *      基于窗口
 *      基于状态
 *          -IntervalJoin
 *          -语法：
 *              keyedA
 *                  .intervalJoin(keyedB)
 *                  .between(下界,上界)
 *                  。process()
 *          -底层原理
 *              connect + 状态
 *          -处理流程
 *              判断是否迟到
 *              将当前处理的数据放到状态中缓存起来
 *              用当前这条数据和另外一条流中缓存的数据进行关联
 *              清状态
 *     局限性： 只支持内连接
 *
 *                              左表                          右表
 *  内连接                 onCreateAndWrite            onCreateAndWrite
 *  左外连接               onReadAndWrite              onCreateAndWrite
 *  右外连接               onCreateAndWrite            onReadAndWrite
 *  全外连接               onReadAndWrite              onReadAndWrite
 */
public class Test01_Join {
    public static void main(String[] args) {
        //TODO 1.基本环境的准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 设置表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.4 设置状态的失效时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //TODO 2.检查点相关的设置（略）
        //TODO 3.从指定的网络端口中读取员工数据 创建动态表
        SingleOutputStreamOperator<Emp> empDS = env
                .socketTextStream("hadoop102", 8888)
                .map(
                        new MapFunction<String, Emp>() {
                            @Override
                            public Emp map(String str) throws Exception {
                                String[] fieldArr = str.split(",");
                                return new Emp(Integer.valueOf(fieldArr[0]), fieldArr[1], Integer.valueOf(fieldArr[2]), Long.valueOf(fieldArr[3]));
                            }
                        }
                );
        tableEnv.createTemporaryView("emp",empDS);
        //TODO 4.从指定的网络端口中读取部门数据 创建动态表
        SingleOutputStreamOperator<Dept> deptDS = env
                .socketTextStream("hadoop102", 8889)
                .map(
                        new MapFunction<String, Dept>() {
                            @Override
                            public Dept map(String str) throws Exception {
                                String[] fieldArr = str.split(",");
                                return new Dept(Integer.valueOf(fieldArr[0]), fieldArr[1], Long.valueOf(fieldArr[2]));
                            }
                        }
                );
        tableEnv.createTemporaryView("dept",deptDS);
        //TODO 5.内连接
        //注意：在使用FlinkSQL普通内外连接的时候，flink会为参与连接的两张表各自维护一个状态，用于存放两张表的数据
        //默认情况下，状态永远不会失效。在实际环境中，必须要设置状态的失效时间(TTL)
        //tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10))
        //tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e join dept d on e.deptno = d.deptno").print();

        //TODO 6.左外连接
        //如果左表数据先到，右表数据后到，会产生3条数据
        //左表    null        +I
        //左表    null        -D
        //左表    右表        +I
        //这样的动态表转换的流称之为回撤流
        //如果将这样的结果写到kafka主题中，kafka主题会接收到3条消息
        //左表        null
        //null
        //左表        右表
        //如果使用FlinkSQL的方式从kafka主题中读取数据，会自动的过滤掉null消息
        //如果使用FlinkAPI的方式从kafka主题中读取数据，默认的SimpleStringSchema是不能处理空消息的，需要自定义反序列化操作
        //在dws中，如果从这样的kafka主题中读取数据，需要去重

        //tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e left join dept d on e.deptno = d.deptno").print();

        //TODO 7.右外连接
        //tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e right join dept d on e.deptno = d.deptno").print();

        //TODO 8.全外连接
        //tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e full join dept d on e.deptno = d.deptno").print();
        //TODO 9.将左外连接关联结果写到kafka主题
        //9.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE emp_dept (\n" +
                "  empno integer,\n" +
                "  ename string,\n" +
                "  deptno integer,\n" +
                "  dname string,\n" +
                "   PRIMARY KEY (empno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");
        //9.2 写入
        tableEnv.executeSql("insert into emp_dept select e.empno,e.ename,d.deptno,d.dname from emp e left join dept d on e.deptno = d.deptno");


    }
}
