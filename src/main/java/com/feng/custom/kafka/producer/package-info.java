/**
 * @date 2020/9/3
 * {@link org.apache.kafka.clients.producer.KafkaProducer}
 * KafkaProducer是线程安全的, 而且多个线程使用一个实例比每个线程使用一个实例更快。
 * KafkaProducer使用缓冲池维护records, 后台I/O线程负责发送这些records至kafka集群。
 * 使用完成后需关闭producer, 否则将会造成内存泄漏。
 * <p>
 * 幂等发送和事务发送。 TODO
 * <p>
 * KafkaProducer的配置文档: <a href="http://kafka.apache.org/documentation.html#producerconfigs">Kafka documentation</a>
 * <p>
 * {@link org.apache.kafka.clients.producer.ProducerRecord}
 * 发送给kafka的key/value pair, 由topic、可选的partition、可选的key、value组成。
 * <p>
 * <p>
 * 发送消息有三种方式:
 * 发送并忘记({@link com.feng.custom.kafka.producer.FireAndForgetProducer})
 * 同步发送({@link com.feng.custom.kafka.producer.SyncProducer})
 * 异步发送({@link com.feng.custom.kafka.producer.AsyncProducer})
 */
package com.feng.custom.kafka.producer;