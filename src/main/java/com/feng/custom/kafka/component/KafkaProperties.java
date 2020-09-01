package com.feng.custom.kafka.component;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @date 2020/9/1
 */
public class KafkaProperties {
    private static final Object LOCK = new Object();
    private static volatile KafkaProperties kafkaProperties;

    /**
     * 指定broker的地址清单, 至少提供两个broker的信息, 一旦其中一个宕机, 生产者仍然能够连接到集群上。
     */
    private String address;

    /**
     * 标识消息的来源, 通常用在日志, 度量指标和配额里。
     */
    private String clientId;

    private KafkaProperties() {
    }

    public static KafkaProperties getInstance() {
        if (kafkaProperties == null) {
            synchronized (LOCK) {
                if (kafkaProperties == null) {
                    kafkaProperties = new KafkaProperties();
                    Properties properties = new Properties();
                    InputStream inputStream = KafkaProperties.class.getClassLoader().getResourceAsStream("kafka.properties");
                    try {
                        properties.load(inputStream);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    kafkaProperties.setAddress(properties.getProperty("kafka.address", "127.0.0.1:9092"));
                    kafkaProperties.setClientId(properties.getProperty("kafka.client.id", "default_client_id"));
                }
            }
        }
        return kafkaProperties;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
}
