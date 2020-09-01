package com.feng.custom.kafka.producer;

import com.feng.custom.kafka.component.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


/**
 * @date 2020/9/1
 * <p>
 *
 * <p>
 * KafkaProducer一般会发生两类错误。
 * 其中一类是可重试错误, 这类错误可以通过重发消息来解决。比如对于连接错误, 可以通过再次建立连接来解决, "无主(no leader)" 错误则可以通过重新为分区选举首领来解决。
 * KafkaProducer可以被配置成自动重试, 如果在多次重试后仍不能解决问题, 应用程序会收到一个重试异常。
 * 另一类错误无出通过重试解决, 比如"消息太大"异常。对于这类错误, KafkaProducer不会进行任何重试, 直接抛出异常。
 */
public abstract class ProducerBase {
    protected KafkaProperties kafkaProperties;
    protected KafkaProducer<String, String> kafkaProducer;

    protected KafkaProducer<String, String> init() {
        kafkaProperties = KafkaProperties.getInstance();
        Properties properties = new Properties();

        // 配置broker的地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getAddress());

        // 配置键序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 配置值序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 配置client.id
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaProperties.getClientId());
        kafkaProducer = new KafkaProducer<>(properties);
        return kafkaProducer;
    }

    public KafkaProducer<String, String> getKafkaProducer() {
        return kafkaProducer;
    }

    public void setKafkaProducer(KafkaProducer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    // TODO close
}
