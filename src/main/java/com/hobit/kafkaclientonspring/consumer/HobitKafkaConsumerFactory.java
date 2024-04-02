package com.hobit.kafkaclientonspring.consumer;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class HobitKafkaConsumerFactory {
    private final List<HobitKafkaConsumer> consumerPool = new ArrayList<>();

    public void createAndStartConsumer(String bootstrapServers, String groupId, String topic) {
        HobitKafkaConsumer consumer = new HobitKafkaConsumer(bootstrapServers, groupId, topic);
        consumer.start();
        consumerPool.add(consumer);
    }

    public void shutdownAll() {
        for (HobitKafkaConsumer consumer : consumerPool) {
            consumer.shutdown();
        }
    }

    public void shutdownConsumersByTopic(String topic) {
        for (HobitKafkaConsumer consumer : consumerPool) {
            if (consumer.getTopic().equals(topic)) { consumer.shutdown(); }
        }
    }
}