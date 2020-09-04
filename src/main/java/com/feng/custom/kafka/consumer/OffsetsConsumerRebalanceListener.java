package com.feng.custom.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * @date 2020/9/4
 * <p>
 * 把偏移量和数据处理保存到同一个外部系统, 实现单次语义。
 */
public class OffsetsConsumerRebalanceListener<K, V> implements ConsumerRebalanceListener {
    private static final Logger logger = LoggerFactory.getLogger(OffsetsConsumerRebalanceListener.class);

    private final KafkaConsumer<K, V> kafkaConsumer;

    // KafkaConsumer在单线程中使用, 该map用于保存分区已处理完的的偏移量。
    private final Map<TopicPartition, Long> topicPartitionMap;

    private volatile boolean assignedFlag;

    public OffsetsConsumerRebalanceListener(KafkaConsumer<K, V> kafkaConsumer, Map<TopicPartition, Long> topicPartitionMap) {
        this.kafkaConsumer = kafkaConsumer;
        this.topicPartitionMap = topicPartitionMap;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        assignedFlag = false;
        logger.info("onPartitionsRevoked, partitions: {}, topicPartitionMap: {}", partitions, topicPartitionMap);

        // 使用topicPartitionMap, 可将已处理的分区对应的偏移量保存至数据库
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        assignedFlag = true;
        logger.info("onPartitionsAssigned, partitions: {}", partitions);

        // 从数据库中读取分区对应偏移量, 此处直接设置对应分区从起始位置开始读取消息。
        kafkaConsumer.seekToBeginning(partitions);
    }

    public boolean isAssignedFlag() {
        return assignedFlag;
    }

    public void setAssignedFlag(boolean assignedFlag) {
        this.assignedFlag = assignedFlag;
    }
}
