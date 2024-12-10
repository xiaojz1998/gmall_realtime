package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DimMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

/**
 * @author Felix
 * @date 2024/12/09
 * sku粒度下单聚合统计
 * 需要启动的进程
 *      zk、kafka、maxwell、hdfs、hbase、redis、doris、DwdTradeOrderDetail、DwsTradeSkuOrderWindow
 */
public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindow().start(
                10029,
                4,
                "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.对流中数据进行类型转换并过滤掉空消息
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
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS
                = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        //TODO 3.去重
        //3.1 状态 + 定时器   优点：即使出现了重复数据，只会向下游传递1条数据，不会出现数据膨胀   缺点:不管是否出现重复，都要等5s后才能向下游传递数据，时效性差
        /*
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上条数据
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if(lastJsonObj == null){
                            //没有重复
                            //将当前数据放到状态中保留起来
                            lastJsonObjState.update(jsonObj);
                            //注册5s之后执行的定时器
                            TimerService timerService = ctx.timerService();
                            long currentProcessingTime = timerService.currentProcessingTime();
                            timerService.registerProcessingTimeTimer(currentProcessingTime + 5000L);
                        }else {
                            //重复了   伪代码
                            String ts1 = lastJsonObj.getString("聚合时间");
                            String ts2 = jsonObj.getString("聚合时间");
                            if(ts2.compareTo(ts1) >= 0){
                                lastJsonObjState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        //定时器触发的时候，执行的方法
                        JSONObject jsonObj = lastJsonObjState.value();
                        out.collect(jsonObj);
                        //清状态里的数据
                        lastJsonObjState.clear();
                    }
                }
        );
        */
        //3.2 状态 + 抵消   优点：时效性强     缺点：如果出现重复，需要向下游传递3条数据(数据膨胀)
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
                        //从状态中获取上条数据
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if(lastJsonObj != null){
                            //重复  将状态中影响到度量值的字段进行取反，再向下游传递
                            String splitOriginalAmount = jsonObj.getString("split_original_amount");
                            String splitCouponAmount = jsonObj.getString("split_coupon_amount");
                            String splitActivityAmount = jsonObj.getString("split_activity_amount");
                            String splitTotalAmount = jsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_original_amount","-" + splitOriginalAmount);
                            lastJsonObj.put("split_coupon_amount","-" + splitCouponAmount);
                            lastJsonObj.put("split_activity_amount","-" + splitActivityAmount);
                            lastJsonObj.put("split_total_amount","-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );
        //distinctDS.print();

        //TODO 4.再次对流中数据进行类型转换  jsonObj->实体类对象
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = distinctDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) throws Exception {
                        //{"create_time":"2024-12-09 14:20:10","sku_num":"1","activity_rule_id":"1","split_original_amount":"6999.0000","split_coupon_amount":"0.0",
                        // "sku_id":"1","date_id":"2024-12-09","user_id":"2028","province_id":"15","activity_id":"1","sku_name":"小米12S",
                        // "id":"14024","order_id":"9893","split_activity_amount":"500.0","split_total_amount":"6499.0","ts":1733725210}
                        String skuId = jsonObj.getString("sku_id");
                        BigDecimal splitOriginalAmount = jsonObj.getBigDecimal("split_original_amount");
                        BigDecimal splitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
                        BigDecimal splitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        Long ts = jsonObj.getLong("ts") * 1000;
                        return TradeSkuOrderBean.builder()
                                .skuId(skuId)
                                .originalAmount(splitOriginalAmount)
                                .couponReduceAmount(splitCouponAmount)
                                .activityReduceAmount(splitActivityAmount)
                                .orderAmount(splitTotalAmount)
                                .ts(ts)
                                .build();
                    }
                }
        );
        //beanDS.print();
        //TODO 5.指定Watermark的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<TradeSkuOrderBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeSkuOrderBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeSkuOrderBean>() {
                                    @Override
                                    public long extractTimestamp(TradeSkuOrderBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );
        //TODO 6.按照维度sku进行分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS
                = withWatermarkDS.keyBy(TradeSkuOrderBean::getSkuId);
        //TODO 7.开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS
                = skuIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        //TODO 8.聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean orderBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        out.collect(orderBean);
                    }
                }
        );
        //reduceDS.print();
        //TODO 9.关联sku维度
        /*
        //V1.0  维度关联的最基本的实现
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    Connection hBaseConn;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hBaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hBaseConn);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        //根据主键到HBase表中获取对应的维度数据
                        //id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time
                        JSONObject dimJsonObj = HBaseUtil.getRow(hBaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class);
                        //将维度属性补充到流中对象上
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        return orderBean;
                    }
                }
        );

        //V2.0  旁路缓存
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    Connection hBaseConn;
                    Jedis jedis;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hBaseConn = HBaseUtil.getHBaseConnection();
                        jedis = RedisUtil.getJedis();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hBaseConn);
                        RedisUtil.closeJedis(jedis);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        //根据流中对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        //先从缓存中获取对应的维度数据
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, "dim_sku_info", skuId);
                        if(dimJsonObj != null){
                            //如果在缓存中，找到了对应的维度，直接将其进行返回(缓存命中)
                            System.out.println("从Redis中获取到了维度数据");
                        }else{
                            //如果从缓存中没有找到要关联的维度，发送请求到HBase中查找
                            dimJsonObj = HBaseUtil.getRow(hBaseConn,Constant.HBASE_NAMESPACE,"dim_sku_info",skuId, JSONObject.class);
                            if(dimJsonObj != null){
                                //将查询到的数据放到Redis中缓存起来，方便下次查询使用
                                System.out.println("从HBase中获取到了维度数据");
                                RedisUtil.writeDim(jedis,"dim_sku_info",skuId,dimJsonObj);
                            }else {
                                System.out.println("没有找到要关联的维度数据");
                            }
                        }
                        if(dimJsonObj != null){
                            //将维度属性补充到流中对象上
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        }
                        return orderBean;
                    }
                }
        );
         */
        //V3.0 旁路缓存 + 模板方法
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new DimMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }
                }
        );
        withSkuInfoDS.print();

        //TODO 10.关联spu维度


        //TODO 11.关联tm维度
        //TODO 12.关联category3维度
        //TODO 13.关联category2维度
        //TODO 14.关联category1维度
        //TODO 15.将关联的结果写到Doris中
    }
}
