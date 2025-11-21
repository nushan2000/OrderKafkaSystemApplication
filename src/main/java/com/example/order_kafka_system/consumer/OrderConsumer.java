package com.example.order_kafka_system.consumer;

import com.example.order_kafka_system.model.Order;
import com.example.order_kafka_system.producer.OrderProducer;
import com.example.order_kafka_system.service.PriceAggregationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderConsumer {

    private final PriceAggregationService priceAggregationService;
    private final OrderProducer orderProducer;

    private final ThreadLocal<AtomicInteger> retryCount =
            ThreadLocal.withInitial(() -> new AtomicInteger(0));

    @KafkaListener(topics = "orders-topic", groupId = "order-processing-group")
    public void consumeOrder(Order order, Acknowledgment ack) {
        try {
            log.info("Received order: {} for product: {} with price: {}",
                    order.getOrderId(), order.getProduct(), order.getPrice());

            processOrderWithRetry(order);

            // Update running average
            priceAggregationService.updateRunningAverage(order.getPrice());

            ack.acknowledge();
            retryCount.get().set(0); // Reset retry count on success

        } catch (Exception e) {
            log.error("Error processing order: {}", order.getOrderId(), e);
            handleProcessingFailure(order, e.getMessage(), ack);
        }
    }

    @Retryable(
            retryFor = {RuntimeException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000)
    )
    private void processOrderWithRetry(Order order) {
        try {
            // Simulate processing logic
            processOrder(order);
        } catch (TemporaryFailureException e) {
            int currentRetry = retryCount.get().incrementAndGet();
            log.warn("Temporary failure processing order: {}, retry count: {}",
                    order.getOrderId(), currentRetry);
            throw e; // This will trigger retry
        } catch (PermanentFailureException e) {
            log.error("Permanent failure for order: {}", order.getOrderId());
            orderProducer.sendToDlq(order, e.getMessage());
            throw new RuntimeException("Permanent failure - sent to DLQ");
        }
    }

    private void processOrder(Order order) {
        // Simulate different types of failures
        if (order.getPrice() < 0) {
            throw new PermanentFailureException("Invalid price: " + order.getPrice());
        }

//        if (order.getProduct().contains("TEMPORARY_ERROR")) {
//            throw new TemporaryFailureException("Temporary processing error");
//        }

        // Simulate successful processing
        log.info("Successfully processed order: {}", order.getOrderId());
    }

    private void handleProcessingFailure(Order order, String error,
                                         Acknowledgment ack) {
        if (retryCount.get().get() >= 3) {
            log.error("Max retries exceeded for order: {}, sending to DLQ",
                    order.getOrderId());
            orderProducer.sendToDlq(order, error);
        }
        ack.acknowledge();
    }

    // Exception classes - make them public static
    public static class TemporaryFailureException extends RuntimeException {
        public TemporaryFailureException(String message) { super(message); }
    }

    public static class PermanentFailureException extends RuntimeException {
        public PermanentFailureException(String message) { super(message); }
    }
}