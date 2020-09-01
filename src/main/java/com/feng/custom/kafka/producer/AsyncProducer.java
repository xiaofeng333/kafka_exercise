package com.feng.custom.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @date 2020/9/1
 */
public class AsyncProducer extends ProducerBase {
    private static final Logger logger = LoggerFactory.getLogger(FireAndForgetProducer.class);

    public static void main(String[] args) {
        AsyncProducer asyncProducer = new AsyncProducer();
        asyncProducer.init();
        asyncProducer.send("test", "async", "async", (metadata, exception) -> {
            logger.info("metadata: {}", metadata);
            if (exception != null) {
                logger.error("error happen", exception);
            }
        });
    }

    /**
     * 发送消息
     *
     * @param topic    目标主题
     * @param key      键
     * @param data     发送的内容
     * @param callback 回调函数, 服务器在返回响应时调用该函数。
     */
    public void send(String topic, String key, String data, Callback callback) {

        // 键和值对象的类型必须与序列化器和生产者对象相匹配
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, data);
        kafkaProducer.send(producerRecord, callback);
    }
}
