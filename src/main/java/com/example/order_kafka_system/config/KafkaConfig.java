package com.example.order_kafka_system.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${app.kafka.topics.orders}")
    private String ordersTopic;

    @Value("${app.kafka.topics.orders-dlq}")
    private String ordersDlqTopic;

    @Value("${app.kafka.topics.orders-retry}")
    private String ordersRetryTopic;

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(ordersTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic ordersDlqTopic() {
        return TopicBuilder.name(ordersDlqTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic ordersRetryTopic() {
        return TopicBuilder.name(ordersRetryTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}