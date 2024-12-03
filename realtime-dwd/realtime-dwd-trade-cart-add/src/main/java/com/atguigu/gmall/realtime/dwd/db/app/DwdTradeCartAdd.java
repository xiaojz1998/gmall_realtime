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
 * 加购事实表
 */
public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(
                10013,
                4,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        //TODO 1.从kafka的topic_db主题中读取数据创建动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_CART_ADD);
        //TODO 2.过滤出加购行为
        Table cartTable = tableEnv.sqlQuery("select \n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    if(`type`='insert',`data`['sku_num'],cast((CAST(`data`['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) as string)) sku_num,\n" +
                "    ts\n" +
                "from topic_db \n" +
                "where `table`='cart_info' \n" +
                "    and (`type`='insert' \n" +
                "    or (`type`='update' and `old`['sku_num'] is not null \n" +
                "    and CAST(`data`['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT)))");
        //cartTable.execute().print();

        //TODO 3.将加购数据写到kafka主题
        //3.1 创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE "+Constant.TOPIC_DWD_TRADE_CART_ADD+" (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
        //3.2 写入
        cartTable.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
