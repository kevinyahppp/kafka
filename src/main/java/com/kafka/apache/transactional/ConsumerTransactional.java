package com.kafka.apache.transactional;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Component
@Slf4j
public class ConsumerTransactional {
    public void consume() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("group.id","devs4j-group");
        props.setProperty("enable.auto.commit","true");
        props.setProperty("isolation.level","read_committed");
        props.setProperty("auto.commit.interval.ms","1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try(Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);) {
            consumer.subscribe(Arrays.asList("devs4j-topic"));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Offset={}, Partition={}, Key={}, Value={}", consumerRecord.offset(),
                            consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                }
            }
        }
    }
}
