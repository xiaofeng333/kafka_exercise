package com.feng.custom.kafka.consumer;

import com.feng.custom.kafka.component.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @date 2020/9/3
 */
public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
        KafkaConsumer<String, String> kafkaConsumer = consumer.initKafkaConsumer();

        Map<TopicPartition, Long> topicPartitionMap = new HashMap<>();
        OffsetsConsumerRebalanceListener<String, String> offsetsConsumerRebalanceListener = new OffsetsConsumerRebalanceListener<>(kafkaConsumer, topicPartitionMap);

        // 订阅主题, 也可以传入正则表达式, 匹配多个主题。
        kafkaConsumer.subscribe(Collections.singleton("test"), offsetsConsumerRebalanceListener);

        // TODO 查看源码, subscribe和assign是懒加载, 如何使他们加载且设置偏移量后, 再开始poll
        kafkaConsumer.poll(Duration.ofMillis(5000));

        try {
            while (offsetsConsumerRebalanceListener.isAssignedFlag()) {
                try {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMinutes(1));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("record: {}", record);
                        topicPartitionMap.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
                    }
                } catch (WakeupException e) {
                    break;
                }
            }
        } finally {
            kafkaConsumer.close();
        }
    }

    public KafkaConsumer<String, String> initKafkaConsumer() {
        KafkaProperties kafkaProperties = KafkaProperties.getInstance();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getAddress());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, getClass().getName());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaProperties.getClientId());
        return new KafkaConsumer<>(properties);
    }
}
