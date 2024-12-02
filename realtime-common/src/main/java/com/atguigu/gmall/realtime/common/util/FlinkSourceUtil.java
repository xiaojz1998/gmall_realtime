package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.util.Properties;

/**
 * @author Felix
 * @date 2024/11/30
 * 获取flink相关source的工具类
 */
public class FlinkSourceUtil {
    //获取kafkasource
    public static KafkaSource<String> getKafkaSource(String topic,String groupId){
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setStartingOffsets(OffsetsInitializer.latest())
                //.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
                //注意：如果使用Flink提供的针对字符串进行反序列化的SimpleStringSchema，不能处理从kafka读取到的空消息
                //如果要向处理空消息，需要自定义反序列化的实现
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if(message != null){
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        return kafkaSource;
    }

    //获取MySqlSource
    public static MySqlSource<String> getMySqlSource(String tableName){
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList("gmall0620_config")
                .tableList("gmall0620_config." + tableName)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .jdbcProperties(props)
                .build();
        return mySqlSource;
    }
}
