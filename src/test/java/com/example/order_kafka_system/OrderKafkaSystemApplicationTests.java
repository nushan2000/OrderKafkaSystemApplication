package com.example.order_kafka_system;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class OrderKafkaSystemApplicationTests {

	@Test
	void contextLoads() {
	}

}
