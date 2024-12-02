package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

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
}
