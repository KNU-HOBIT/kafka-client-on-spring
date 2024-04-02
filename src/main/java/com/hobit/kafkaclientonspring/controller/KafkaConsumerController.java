package com.hobit.kafkaclientonspring.controller;

import com.hobit.kafkaclientonspring.service.HobitKafkaConsumerService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.beans.factory.annotation.Autowired;

@RestController
public class KafkaConsumerController {

    private final HobitKafkaConsumerService hobitKafkaConsumerService;

    @Autowired
    public KafkaConsumerController(HobitKafkaConsumerService kafkaConsumerService) {
        this.hobitKafkaConsumerService = kafkaConsumerService;
    }

    @PostMapping("/create-consumer")
    public String createConsumer(@RequestParam("bootstrapServers") String bootstrapServers,
                                 @RequestParam("group") String group,
                                 @RequestParam("topic") String topic
                                 ) {
        hobitKafkaConsumerService.createConsumer(bootstrapServers, group, topic);
        return "Consumer created for topic: " + topic + ", group: " + group;
    }

    @PostMapping("/shutdown-consumers-by-topic")
    public String shutdownConsumerByTopic(@RequestParam("topic") String topic) {
        hobitKafkaConsumerService.shutdownConsumersByTopic(topic);
        return "Consumers shut down";
    }

    @PostMapping("/shutdown-all-consumers")
    public String shutdownConsumers() {
            hobitKafkaConsumerService.shutdownAllConsumers();
            return "Consumers shut down";
    }
}

