package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author Felix
 * @date 2024/12/02
 * 获取Flink相关Sink的工具类
 */
public class FlinkSinkUtil {
    //获取KafkaSink
    public static KafkaSink<String> getKafkaSink(String topic){
        /* Flink操作kafka的一致性的保证
         *      从kafka中读取数据
         *          KafkaSource->KafkaSourceReader->offsetsToCommit
         *      向kafka中写入数据
         *          开启检查点
         *          设置一致性级别
         *              setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
         *          设置事务的超时时间
         *              检查点超时时间 < 事务的超时时间<= kafka事务的最大超时时间(默认15min)
         *          设置事务id的前缀
         *              .setTransactionalIdPrefix("xxx")
         *          在消费端，需要设置消费的隔离级别为读已提交
         *              setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
         * */
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000 + "")
                //.setTransactionalIdPrefix("xxx")
                .build();
        return kafkaSink;
    }
    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink(){

        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink = KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> tup2, KafkaSinkContext context, Long timestamp) {
                        JSONObject jsonObj = tup2.f0;
                        TableProcessDwd tableProcessDwd = tup2.f1;
                        String topic = tableProcessDwd.getSinkTable();
                        return new ProducerRecord<byte[], byte[]>(topic,jsonObj.toJSONString().getBytes());
                    }
                })
                .build();
        return kafkaSink;
    }

    public static <T>KafkaSink<T> getKafkaSink(KafkaRecordSerializationSchema<T> krs){
        KafkaSink<T> kafkaSink = KafkaSink.<T>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(krs)
                .build();
        return kafkaSink;
    }

    //获取DorisSink
    public static DorisSink<String> getDorisSink(String tableName){
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        DorisSink<String> dorisSink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes(Constant.DORIS_FE_NODES)
                        .setTableIdentifier(Constant.DORIS_DATABASE + "." + tableName)
                        .setUsername("root")
                        .setPassword("aaaaaa")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
        return dorisSink;
    }
}
