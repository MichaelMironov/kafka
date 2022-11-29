package org.example;

import io.kcache.Cache;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class App {
    final static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws IOException {
        String bootstrapServer = "localhost:9092";
        String topic = "credit_cards";
        Properties properties = new Properties();
        properties.setProperty("kafkacache.bootstrap.servers", bootstrapServer);
        properties.setProperty("kafkacache.topic", topic);

        Cache<String, String> cache = new KafkaCache<String, String>(
                new KafkaCacheConfig(properties), Serdes.String(), Serdes.String());

        cache.init();

        cache.put("client_1", "4303 2036 3891 1954");
        cache.put("client_2", "4177 6829 7065 9524");
        cache.put("client_3", "4425 6007 4689 1874");
        cache.put("client_4", "4569 7374 2626 5213");

        logger.info(cache.get("client_2"));

        cache.close();
    }

}
