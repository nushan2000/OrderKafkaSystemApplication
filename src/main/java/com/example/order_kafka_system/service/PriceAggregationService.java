package com.example.order_kafka_system.service;


import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class PriceAggregationService {

    private final AtomicLong totalPrice = new AtomicLong(0);
    private final AtomicInteger orderCount = new AtomicInteger(0);

    @Getter
    private volatile double runningAverage = 0.0;

    public void updateRunningAverage(float price) {
        long currentTotal = totalPrice.addAndGet((long) (price * 100)); // Store as cents
        int count = orderCount.incrementAndGet();

        this.runningAverage = (double) currentTotal / count / 100;

        log.info("Updated running average: {}. Total orders: {}",
                String.format("%.2f", runningAverage), count);
    }

    @Scheduled(fixedRate = 5000) // Log every 5 seconds
    public void logCurrentStats() {
        log.info("=== Current Statistics ===");
        log.info("Running average price: {}", String.format("%.2f", runningAverage));
        log.info("Total orders processed: {}", orderCount.get());
        log.info("==========================");
    }
}