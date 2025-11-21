package com.example.order_kafka_system;

import org.springframework.boot.SpringApplication;

public class TestOrderKafkaSystemApplication {

	public static void main(String[] args) {
		SpringApplication.from(OrderKafkaSystemApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
