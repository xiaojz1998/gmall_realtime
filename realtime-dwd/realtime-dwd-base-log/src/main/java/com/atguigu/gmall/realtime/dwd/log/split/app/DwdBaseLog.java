package com.atguigu.gmall.realtime.dwd.log.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Felix
 * @date 2024/12/02
 * 日志分流
 * 需要启动的进程
 *      zk、kafka、flume、DwdBaseLog
 */
public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011,4, "dwd_base_log",Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.对流中数据进行类型转换并ETL  jsonStr->jsonObj  如果是脏数据放到侧流中--->kafka
        //1.1 定义侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};

        //1.2 etl
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            //如果没有发生异常，说明是标准的json  将数据向下游传递
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            //如果发生了异常，说明不是标准的json，将其放到侧流
                            ctx.output(dirtyTag,jsonStr);
                        }
                    }
                }
        );
        //jsonObjDS.print("标准:");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        //dirtyDS.print("脏数据:");
        //1.3 将脏数据写到kafka主题中
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);

        //TODO 2.新老访客标记修复
        /*
         * Flink的状态编程
         *      状态：用于保存程序运行的中间结果
         *      分类：
         *          原始状态
         *          托管状态
         *              算子状态:算子的每一个并行子任务
         *                      List、unionList、广播
         *              键控状态:经过keyby之后的每一个分组
         *                      ValueState、ListState、MapState、ReducingState、AggregatingState
         *                      声明状态
         *                      在open方法中对状态进行初始化
         */
        //2.1 按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //2.2 使用Flink的状态编程修复新老访客标记
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        //valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                        //                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        //        .build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //获取is_new的值
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        //从状态中获取上次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        //获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        //将毫秒转换为年月日形式的字符串
                        String curVisitDate = DateFormatUtil.tsToDate(ts);

                        if ("1".equals(isNew)) {
                            //如果is_new的值为1
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                //如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    //如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            //如果 is_new 的值为 0
                            //如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，
                            // 日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。
                            // 本程序选择将状态更新为昨日；
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterDay);
                            }
                        }

                        out.collect(jsonObj);
                    }
                }
        );

        fixedDS.print();

        //TODO 3.分流    错误日志-错误侧输出流  启动日志-启动侧输出流  曝光日志-曝光侧输出流  动作日志-动作侧输出流  页面日志-主流
        //3.1 定义侧输出流标签
        //3.2 分流
        //3.3 将不同流的数据写到kafka不同主题中


    }

}
