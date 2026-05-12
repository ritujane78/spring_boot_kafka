package com.jane.kafka;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.Async;

@SpringBootApplication
public class KafkaStreamApplication {



	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamApplication.class, args);
	}



}
