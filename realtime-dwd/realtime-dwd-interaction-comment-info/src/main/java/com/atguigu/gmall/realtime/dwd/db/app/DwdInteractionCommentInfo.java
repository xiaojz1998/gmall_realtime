package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2024/12/03
 * 评论事实表
 * 需要启动的进程
 *      zk、kafka、maxwell、hdfs、hbase、DwdInteractionCommentInfo
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012,4,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        //TODO 1.从kafka主题中读取数据创建动态表  并指定处理时间、事件时间以及WM的生成策略  ---kafka连接器
        readOdsDb(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        //TODO 2.过滤出评论数据
        Table commentTable = tableEnv.sqlQuery("select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['appraise'] appraise,\n" +
                "    `data`['comment_txt'] comment_txt,\n" +
                "   pt,\n" +
                "   ts\n" +
                "from topic_db where `table`='comment_info' and `type`='insert'");
        //commentTable.execute().print();
        tableEnv.createTemporaryView("comment_table",commentTable);
        //tableEnv.executeSql("select * from comment_table").print();

        //TODO 3.从HBase中查询字典数据创建动态表---hbase连接器
        readBaseDic(tableEnv);

        //TODO 4.将评论和字典表进行关联---LookupJoin
        Table joinedTable = tableEnv.sqlQuery("SELECT \n" +
                "    id,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    appraise,\n" +
                "    dic.dic_name appraise_name,\n" +
                "    comment_txt,\n" +
                "    ts\n" +
                "FROM comment_table AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.pt AS dic\n" +
                "    ON c.appraise = dic.dic_code ");
        //joinedTable.execute().print();

        //TODO 5.将关联的结果写到Kafka主题---upsert-kafka连接器
        //5.1 创建动态表和要写入的kafka主题进行映射
        tableEnv.executeSql("CREATE TABLE "+Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    appraise string,\n" +
                "    appraise_name  string,\n" +
                "    comment_txt  string,\n" +
                "    ts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        //5.2 写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}
