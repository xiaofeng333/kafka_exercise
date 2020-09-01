package com.feng.custom.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @date 2020/9/1
 * <p>
 * 发送并忘记, 把消息发送给服务器, 但并不关心它是否正常到达。
 * 在大多数情况下, 消息会正常到达, 因为kafka是高可用的, 而且生产者会总动尝试重发。
 * 不过使用这种方式有时候也会丢失一些信息。
 */
public class FireAndForgetProducer extends ProducerBase {
    private static final Logger logger = LoggerFactory.getLogger(FireAndForgetProducer.class);

    public static void main(String[] args) {
        FireAndForgetProducer fireAndForgetProducer = new FireAndForgetProducer();
        fireAndForgetProducer.init();
        try {
            fireAndForgetProducer.send("test", "fire_and_forget", "fire_and_forget");
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
     * @throws Exception 有可能是SerializationException(说明序列化消息失败), BufferExhaustedException(说明缓冲区已满), TimeoutException, 或InterruptException(说明发送线程被中断)
     */
    public void send(String topic, String key, String data) throws Exception {

        // 键和值对象的类型必须与序列化器和生产者对象相匹配
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, data);
        kafkaProducer.send(producerRecord);
    }
}
