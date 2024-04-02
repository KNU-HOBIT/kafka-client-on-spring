package com.hobit.kafkaclientonspring.service;

import com.hobit.kafkaclientonspring.consumer.HobitKafkaConsumerFactory;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class HobitKafkaConsumerService {

    private final HobitKafkaConsumerFactory hobitKafkaConsumerFactory;

    @Autowired
    public HobitKafkaConsumerService(HobitKafkaConsumerFactory kafkaConsumerFactory) {
        this.hobitKafkaConsumerFactory = kafkaConsumerFactory;
    }

    public void createConsumer(String bootstrapServers, String group, String topic) {
        hobitKafkaConsumerFactory.createAndStartConsumer(bootstrapServers, group, topic);
        log.info("Consumers initialized.");
    }

    public void shutdownConsumersByTopic(String topic) {
        hobitKafkaConsumerFactory.shutdownConsumersByTopic(topic);
        log.info("Shutdown consumers {} initialized.", topic);
    }

    // Method to shut down all consumers
    public void shutdownAllConsumers() {
        hobitKafkaConsumerFactory.shutdownAll();
        log.info("Shutdown ALL consumers.");
    }

    // Additional methods to interact with KafkaConsumer instances
}
