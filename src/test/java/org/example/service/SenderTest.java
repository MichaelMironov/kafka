package org.example.service;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.of;
import static org.junit.jupiter.api.Assertions.*;

class SenderTest {

    @Test
    void sendMessages() {
        String bootstrapServers = "localhost:9092";
        String topic = "send-topics2";
        Sender sender = new Sender(bootstrapServers, topic);
        sender.sendMessages(Stream.of( "test_1","test_2", "test_3","test_4").toList());

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-consumers");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        ConsumerRecords records = consumer.poll(Duration.ofMillis(20000L));

        System.out.println(records.count());
        consumer.close();

        while(records.iterator().hasNext()){
            System.out.println(records.partitions());
        }
        assertEquals(3, records.count());
    }

    @Test
    void getMessages() {
        String bootstrapServers = "localhost:9092";
        String topic = "read-topic";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer producer = new KafkaProducer<>(properties);
        producer.send(new ProducerRecord(topic,"read"));
        producer.close();

        Receiver receiver = new Receiver(bootstrapServers, topic);

        int size = receiver.getCountMessages();
        assertEquals(1, size);
    }
}