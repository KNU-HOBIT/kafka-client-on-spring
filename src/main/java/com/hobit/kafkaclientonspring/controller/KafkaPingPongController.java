package com.hobit.kafkaclientonspring.controller;

import com.hobit.kafkaclientonspring.service.HobitKafkaPingPongService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.beans.factory.annotation.Autowired;

@RestController
public class KafkaPingPongController {

    private final HobitKafkaPingPongService hobitKafkaPingPongService;

    @Autowired
    public KafkaPingPongController(HobitKafkaPingPongService kafkaConsumerService) {
        this.hobitKafkaPingPongService = kafkaConsumerService;
    }

    @PostMapping("/create-pingpong")
    public String createConsumer(@RequestParam("bootstrapServers") String bootstrapServers,
                                 @RequestParam("group") String group,
                                 @RequestParam("topic") String topic,
                                 @RequestParam("pongTopic") String pongTopic
                                 ) {
        hobitKafkaPingPongService.createConsumer(bootstrapServers, group, topic, pongTopic);
        return "Consumer created for topic: " + topic + ", group: " + group;
    }

    @PostMapping("/shutdown-pingpongs-by-topic")
    public String shutdownConsumerByTopic(@RequestParam("topic") String topic) {
        hobitKafkaPingPongService.shutdownConsumersByTopic(topic);
        return "Consumers shut down";
    }

    @PostMapping("/shutdown-all-pingpongs")
    public String shutdownConsumers() {
            hobitKafkaPingPongService.shutdownAllConsumers();
            return "Consumers shut down";
    }
}

