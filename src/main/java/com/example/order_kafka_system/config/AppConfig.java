package com.example.order_kafka_system.config;


import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableKafkaRetryTopic
@EnableScheduling
public class AppConfig {
}