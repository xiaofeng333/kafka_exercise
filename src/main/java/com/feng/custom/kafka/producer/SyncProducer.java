package com.feng.custom.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

/**
 * @date 2020/9/1
 * <p>
 * 同步发送, 返回Future对象, 调用get()方法进行等待, 就可以知道消息是否发送成功。
 */
public class SyncProducer extends ProducerBase {
    private static final Logger logger = LoggerFactory.getLogger(SyncProducer.class);

    public static void main(String[] args) {
        SyncProducer syncProducer = new SyncProducer();
        syncProducer.init();
        try {
            Future<RecordMetadata> send = syncProducer.send("test", "sync", "sync");
            RecordMetadata recordMetadata = send.get();
            logger.info("send recordMetadata: {}", recordMetadata);
        } catch (Exception e) {
            logger.error("error happen", e);
        }
    }

    /**
     * 发送消息
     *
     * @param topic 目标主题
     * @param key   键
     * @param data  发送的内容
     * @return Future对象, 调用get()方法等待kafka响应。如果服务器返回错误, get()方怯会抛出异常。如果没有发生错误, 我们会得到一个RecordMetadata对象, 可以用它获取消息的偏移量。
     * @throws Exception 抛出异常
     */
    public Future<RecordMetadata> send(String topic, String key, String data) throws Exception {

        // 键和值对象的类型必须与序列化器和生产者对象相匹配
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, data);
        return kafkaProducer.send(producerRecord);
    }
}
