package com.kafka.apache.callback;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
@Slf4j
public class ProducerCallback {
    public void produce() {
        Long starTime = System.currentTimeMillis();
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("acks","1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "10");

        try(org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<String, String>(props);) {
            for (int i = 0; i < 10_000; i++) {
                producer.send(new ProducerRecord<>("devs4j-topic", String.valueOf(i), "devs4j-value"),
                        (recordMetadata, e) -> {
                if (e != null) {
                    log.error("Error ", e.getMessage());
                }
                log.info("Offset={}, Partition={}, Topic={}", recordMetadata.offset(),
                        recordMetadata.partition(), recordMetadata.topic());
                });
            }
            producer.flush();
        }
        log.info("Processing time: {} ms", System.currentTimeMillis() - starTime);
    }
}
