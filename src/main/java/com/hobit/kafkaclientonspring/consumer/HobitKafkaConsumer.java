package com.hobit.kafkaclientonspring.consumer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class HobitKafkaConsumer implements Runnable {
    private final KafkaConsumer<String, String> consumer;

    @Getter
    private final String topic;
    private volatile boolean running = true;

    // Existing constructor and methods...
    public HobitKafkaConsumer(String bootstrapServers, String groupId, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        String shortUUID = UUID.randomUUID().toString().substring(0, 8);
        String clientId = String.format("consumer-for-%s-%s", topic, shortUUID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // 자동 커밋 활성화 여부
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // 초기화 시점부터 메시지 처리
        this.consumer = new KafkaConsumer<>(props);
        this.topic = topic;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(topic));
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            }
        } finally {
            consumer.close();
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        long latency = System.currentTimeMillis() - record.timestamp();

        log.info("\ntopic: {}\noffset: {}\nkey: {}\nvalue: {}\nlatency: {} milliseconds\n{}",
                record.topic(), record.offset(), record.key(), record.value(), latency, "=".repeat(100));
    }

    public void start() { new Thread(this).start(); }  // Ensures that each consumer instance runs in its own threa
    public void shutdown() { running = false; }
}