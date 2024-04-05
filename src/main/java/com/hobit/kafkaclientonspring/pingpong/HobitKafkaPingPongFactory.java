package com.hobit.kafkaclientonspring.pingpong;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class HobitKafkaPingPongFactory {
    private final List<HobitKafkaPingPong> pingPongPool = new ArrayList<>();

    public void createAndStartPingPong(String bootstrapServers, String groupId, String topic, String pongTopic) {
        HobitKafkaPingPong pingPong = new HobitKafkaPingPong(bootstrapServers, groupId, topic, pongTopic);
        pingPong.start();
        pingPongPool.add(pingPong);
    }

    public void shutdownAll() {
        for (HobitKafkaPingPong pingPong : pingPongPool) {
            pingPong.shutdown();
        }
    }

    public void shutdownPingPongsByTopic(String topic) {
        for (HobitKafkaPingPong pingPong : pingPongPool) {
            if (pingPong.getTopic().equals(topic)) { pingPong.shutdown(); }
        }
    }
}