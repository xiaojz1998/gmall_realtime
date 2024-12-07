package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2024/12/07
 * 版本、渠道、地区、新老访客聚合统计
 * 需要启动的进程
 *      zk、kafka、flume、doris、DwdBaseLog、DwsTrafficVcChArIsNewPageViewWindow
 */
public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {

    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                10022,
                4,
                "dws_traffic_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.对流中数据进行类型转换    jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //jsonObjDS.print();

        //TODO 2.按照mid进行分组
        KeyedStream<JSONObject, String> midKeyedDS 
                = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        
        //TODO 3.jsonObj->实体类对象   使用Flink状态编程判断是否为独立访客  相当于WordCount的封装二元组
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = midKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        String vc = commonJsonObj.getString("vc");
                        String ch = commonJsonObj.getString("ch");
                        String ar = commonJsonObj.getString("ar");
                        String isNew = commonJsonObj.getString("is_new");

                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");

                        //判断是否为当天独立访客
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObj.getLong("ts");
                        String curVisitdate = DateFormatUtil.tsToDate(ts);
                        Long uvCt = 0L;
                        if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitdate)) {
                            uvCt = 1L;
                            lastVisitDateState.update(curVisitdate);
                        }
                        //会话计数
                        String lastPageId = pageJsonObj.getString("last_page_id");
                        Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;

                        TrafficPageViewBean viewBean = new TrafficPageViewBean(
                                "",
                                "",
                                "",
                                vc,
                                ch,
                                ar,
                                isNew,
                                uvCt,
                                svCt,
                                1L,
                                pageJsonObj.getLong("during_time"),
                                ts
                        );
                        out.collect(viewBean);
                    }
                }
        );
        //beanDS.print();

        //TODO 4.指定Watermark生成策略以及提取事件时间字段
        /**
         * 关于Flink的水位线
         *      前提：事件时间语义
         *      是一个逻辑时钟
         *      是用于衡量事件时间进展的标记
         *      也会作为流中的元素，随着流的流动向下游传递
         *      主要用于触发窗口的计算以及关闭、定时器执行
         *      是时钟递增不会递减的
         *      在FlinkAPI中，提供了两种水位线的生成策略
         *          单调递增
         *              forMonotonousTimestamps()
         *          有界乱序
         *              forBoundedOutOfOrderness(Duration.ofSeconds(3))
         *          单调递增是有界乱序的子类
         *          单调递增是有界乱序的特殊情况，乱序程度是0
         *      水位线生成原理
         *          实现了WatermarkGenerator
         *              onEvent
         *                  流中每条数据到来的时候，都会被调用的方法
         *                  获取最大时间
         *              onPeriodicEmit
         *                  周期性执行的方法，默认周期200ms
         *                      env.getConfig().setAutoWatermarkInterval()
         *                  创建水位线对象  向下游传递
         *                      output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1))
         *      水位线的传递
         *          上游1个并行度，下游是n个并行--广播
         *          上游n个并行度，下游是1个并行--将上游所有并行度的水位线比较取最小
         *          上游n个并行度，下游是n个并行--先广播再取最小
         *      空闲数据源
         *          .withIdleness(Duration.ofSeconds(60))
         * */
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficPageViewBean>forMonotonousTimestamps()
                        //.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
                        //.withIdleness(Duration.ofSeconds(60))
        );

        //TODO 5.按照统计的维度进行分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> dimKeyedDS = withWatermarkDS.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean bean) throws Exception {
                        return Tuple4.of(
                                bean.getVc(),
                                bean.getCh(),
                                bean.getAr(),
                                bean.getIsNew()
                        );
                    }
                }
        );

        //TODO 6.开窗
        /**
         * 关于FlinkAPI中的窗口
         *      窗口的分类
         *          按照驱动形式分
         *              时间窗口
         *              计数窗口
         *          按照数据划分方式分
         *              滚动
         *              滑动
         *              会话
         *              全局
         *                 计数窗口底层使用的是全局窗口
         *      开窗前是否进行keyBy
         *          keyBy:针对经过keyby之后的每一个组独立开窗，窗口之间相互不影响
         *              window
         *              countWindow
         *          nokeyby:针对整条流进行开窗   并行度强制设置为1
         *              windowAll
         *              countWindowAll
         *      以滚动事件时间窗口为例
         *          窗口对象什么时候创建
         *              属于当前这个窗口的第一个元素到来的时候创建窗口对象
         *          窗口起始时间
         *              向下取整
         *              左闭右开
         *          窗口的结束时间
         *              起始时间 + 窗口大小
         *          窗口的最大时间
         *              窗口结束时间 - 1ms
         *          窗口什么时候触发计算
         *              ctx.getCurrentWatermark() >= window.maxTimestamp()
         *          窗口什么时间关闭
         *              水位线>=窗口最大时间 + 窗口的允许迟到时间
         *              .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
         *      Flink中迟到数据的处理
         *          在执行Watermark生成策略的时候，指定乱序程度
         *          在开窗的时候，设置窗口的允许迟到时间
         *          侧输出流
         */
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS
                = dimKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 7.聚合
        /**
         * 关于FlinkAPI中对窗口数据的处理方式
         *      增量处理    优势：数据来一条处理一条，不会缓存数据，省空间   劣势：不能获取窗口更详细的信息
         *          reduce
         *              窗口中元素类型以及向下游传递的类型一致
         *          aggregate
         *              窗口中元素类型、累加器类型、向下游传递的类型不一致
         *      全量处理    优势：可以获取窗口更详细的信息     劣势：会缓存数据，费空间
         *          apply
         *              参数中接收的是窗口对象
         *          process
         *              参数中接收的是上下文对象，更底层
         *      增量 + 全量
         */
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        return value1;
                    }
                },
                new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                        TrafficPageViewBean viewBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        viewBean.setStt(stt);
                        viewBean.setEdt(edt);
                        viewBean.setCur_date(curDate);
                        //注意：千万别忘了向下游传递数据
                        out.collect(viewBean);
                    }
                }
        );

        //TODO 8.将聚合结果写到Doris中
        reduceDS.print();

        reduceDS
                //将流中实体类对象转换为json格式字符串
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));

    }
}
