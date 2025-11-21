package com.example.order_kafka_system.service;

import com.example.order_kafka_system.model.Order;
import com.example.order_kafka_system.producer.OrderProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class TestDataGenerator {

    private final OrderProducer orderProducer;
    private final AtomicLong orderIdCounter = new AtomicLong(1000);
    private final Random random = new Random();

    private final String[] products = {"Laptop", "Phone", "Tablet", "Headphones", "Monitor"};

    @Scheduled(fixedRate = 2000) // Generate order every 2 seconds
    public void generateTestOrder() {
        Order order = Order.newBuilder()
                .setOrderId(String.valueOf(orderIdCounter.incrementAndGet()))
                .setProduct(products[random.nextInt(products.length)])
                .setPrice(50 + random.nextFloat() * 950) // Prices between 50-1000
                .build();

        orderProducer.sendOrder(order);
        log.info("Generated test order: {}", order.getOrderId());
    }

    // Method to generate specific test cases
    public void generateErrorTestCases() {
        // Generate order with negative price (should go to DLQ)
        Order invalidPriceOrder = Order.newBuilder()
                .setOrderId("INVALID_PRICE")
                .setProduct("Test Product")
                .setPrice(-50.0f)
                .build();
        orderProducer.sendOrder(invalidPriceOrder);

        // Generate order with temporary error
        Order tempErrorOrder = Order.newBuilder()
                .setOrderId("TEMP_ERROR")
                .setProduct("TEMPORARY_ERROR_ITEM")
                .setPrice(100.0f)
                .build();
        orderProducer.sendOrder(tempErrorOrder);
    }
}