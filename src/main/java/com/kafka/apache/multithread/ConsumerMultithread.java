package com.kafka.apache.multithread;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class ConsumerMultithread {
    public void multiConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("group.id","devs4j-group");
        props.setProperty("enable.auto.commit","true");
        props.setProperty("auto.commit.interval.ms","1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
            ConsumerThread consumerThread = new ConsumerThread(new KafkaConsumer<String, String>(props));
            executorService.execute(consumerThread);
        }
        while (!executorService.isTerminated());
    }
}
