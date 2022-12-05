package org.example.service;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Receiver {
    private String topic;
    private Properties properties;

    public Receiver(String bootstrapService, String topic) {
        this.topic = topic;
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapService);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-consumers");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    public int getCountMessages() {
        Consumer consumer = new KafkaConsumer(properties);
        consumer.subscribe(Arrays.asList(topic));
        ConsumerRecords records = consumer.poll(Duration.ofMillis(10000));
        consumer.close();
        return records.count();
    }

}
