package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author Felix
 * @date 2024/12/11
 * 省份下单聚合统计
 * 需要启动的进程
 *      zk、kafka、maxwell、redis、hdfs、hbase、doris、DwdTradeOrderDetail、DwsTradeProvinceOrderWindow
 */
public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(
                10020,
                4,
                "dws_trade_province_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.对流中数据进行类型转换并过滤空消息   jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (StringUtils.isNotEmpty(jsonStr)) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        //jsonObjDS.print();
        //TODO 2.按照唯一键(订单明细id)进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        //TODO 3.去重-状态+抵消
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            //重复   将状态中影响到度量值的字段进行取反并向下游传递
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );
        //distinctDS.print();
        
        //TODO 4.再次对流中数据进行类型转换  jsonObj->实体类对象
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = distinctDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObj) throws Exception {
                        //{"create_time":"2024-12-09 09:12:45","sku_num":"1","activity_rule_id":"1","split_original_amount":"6999.0000","split_coupon_amount":"0.0",
                        // "sku_id":"2","date_id":"2024-12-09","user_id":"3118","province_id":"16","activity_id":"1","sku_name":"小米12S","id":"17892","order_id":"12667",
                        // "split_activity_amount":"500.0","split_total_amount":"6499.0","ts":1733879565}
                        String provinceId = jsonObj.getString("province_id");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        String orderId = jsonObj.getString("order_id");
                        Long ts = jsonObj.getLong("ts") * 1000;
                        return TradeProvinceOrderBean.builder()
                                .provinceId(provinceId)
                                .orderAmount(splitTotalAmount)
                                //注意：这个方式返回的是不可变的set集合，在开窗聚合的时候会报错
                                //.orderIdSet(Collections.singleton(orderId))
                                .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                                .ts(ts)
                                .build();
                    }
                }
        );
        //beanDS.print();
        //TODO 5.指定Watermark的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<TradeProvinceOrderBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeProvinceOrderBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeProvinceOrderBean>() {
                                    @Override
                                    public long extractTimestamp(TradeProvinceOrderBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );
        //TODO 6.分组
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS
                = withWatermarkDS.keyBy(TradeProvinceOrderBean::getProvinceId);
        //TODO 7.开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS
                = provinceIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        //TODO 8.聚合
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                        TradeProvinceOrderBean orderBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        out.collect(orderBean);
                    }
                }
        );
        //reduceDS.print();
        //TODO 9.关联省份维度
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeProvinceOrderBean>() {
                    @Override
                    public void addDims(TradeProvinceOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setProvinceName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public String getRowKey(TradeProvinceOrderBean orderBean) {
                        return orderBean.getProvinceId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //TODO 10.将关联的结果写到Doris
        withProvinceDS.print();
        withProvinceDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));
    }
}
