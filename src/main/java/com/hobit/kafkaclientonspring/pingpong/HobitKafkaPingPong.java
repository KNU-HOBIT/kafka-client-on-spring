package com.hobit.kafkaclientonspring.pingpong;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class HobitKafkaPingPong implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;

    @Getter
    private final String topic;
    private final String pongTopic; // Pong 메시지를 위한 토픽
    private volatile boolean running = true;

    public HobitKafkaPingPong(String bootstrapServers, String groupId, String topic, String pongTopic) {
        this.topic = topic;
        this.pongTopic = pongTopic;

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        String shortUUID = UUID.randomUUID().toString().substring(0, 8);
        String clientId = String.format("pingpong-%s-%s", topic, shortUUID);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.consumer = new KafkaConsumer<>(consumerProps);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(producerProps);
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
            producer.close();
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        // 기존 레코드 처리 로직 유지
//        long latency = System.currentTimeMillis() - record.timestamp();
//        log.info("\ntopic: {}\noffset: {}\nkey: {}\nvalue: {}\nlatency: {} milliseconds\n{}",
//                record.topic(), record.offset(), record.key(), record.value(), latency, "=".repeat(100));

        // Pong 메시지 생성 및 전송
        String pongMessage = "pong," + record.value();
        producer.send(new ProducerRecord<>(pongTopic, record.key(), pongMessage));
    }

    public void start() {
        new Thread(this).start();  // 각 인스턴스를 독립된 스레드에서 실행
    }

    public void shutdown() {
        running = false;
    }
}