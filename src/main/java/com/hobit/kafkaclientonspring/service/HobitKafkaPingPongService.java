package com.hobit.kafkaclientonspring.service;

import com.hobit.kafkaclientonspring.pingpong.HobitKafkaPingPongFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class HobitKafkaPingPongService {

    private final HobitKafkaPingPongFactory hobitKafkaPingPongFactory;

    @Autowired
    public HobitKafkaPingPongService(HobitKafkaPingPongFactory kafkaConsumerFactory) {
        this.hobitKafkaPingPongFactory = kafkaConsumerFactory;
    }

    public void createConsumer(String bootstrapServers, String group, String topic, String pongTopic) {
        hobitKafkaPingPongFactory.createAndStartPingPong(bootstrapServers, group, topic, pongTopic);
        log.info("Consumers initialized.");
    }

    public void shutdownConsumersByTopic(String topic) {
        hobitKafkaPingPongFactory.shutdownPingPongsByTopic(topic);
        log.info("Shutdown consumers {} initialized.", topic);
    }

    // Method to shut down all consumers
    public void shutdownAllConsumers() {
        hobitKafkaPingPongFactory.shutdownAll();
        log.info("Shutdown ALL consumers.");
    }

    // Additional methods to interact with KafkaConsumer instances
}
