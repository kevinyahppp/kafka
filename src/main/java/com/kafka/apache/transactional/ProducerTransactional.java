package com.kafka.apache.transactional;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
@Slf4j
public class ProducerTransactional {
    public void produce() {
        Long starTime = System.currentTimeMillis();
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("acks","all");
        props.put("transactional.id","devs4j-producer-id");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "10");

        try(Producer<String, String> producer = new KafkaProducer<String, String>(props);) {
            try {
                producer.initTransactions();
                producer.beginTransaction();
                for (int i = 0; i < 100_000; i++) {
                    producer.send(new ProducerRecord<>("devs4j-topic", String.valueOf(i), "devs4j-value"));
                    if (i == 50_000) {
                        throw new Exception("Exception throw");
                    }
                }
                producer.commitTransaction();
                producer.flush();
            } catch (Exception e) {
                log.error("Error ", e);
                producer.abortTransaction();
            }
        }
        log.info("Processing time: {} ms", System.currentTimeMillis() - starTime);
    }
}
