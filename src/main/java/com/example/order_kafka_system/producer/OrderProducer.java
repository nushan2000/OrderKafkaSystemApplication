package com.example.order_kafka_system.producer;
import com.example.order_kafka_system.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    public void sendOrder(Order order) {
        String key = order.getOrderId().toString();

        CompletableFuture<SendResult<String, Order>> future =
                kafkaTemplate.send("orders-topic", key, order);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent order: {} with offset: {}",
                        order.getOrderId(), result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send order: {}", order.getOrderId(), ex);
            }
        });
    }

    public void sendToDlq(Order order, String error) {
        log.warn("Sending order {} to DLQ. Error: {}", order.getOrderId(), error);
        kafkaTemplate.send("orders-dlq-topic", order.getOrderId().toString(), order);
    }
}