package com.kafka.apache.multithread;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@RequiredArgsConstructor
public class ConsumerThread extends Thread {
    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    @Override
    public void run() {
        consumer.subscribe(Arrays.asList("devs4j-topic"));
        try {
            while (!closed.get()) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.debug("Offset={}, Partition={}, Key={}, Value={}", consumerRecord.offset(),
                            consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                    if (Integer.parseInt(consumerRecord.key()) % 1000000 == 0) {
                        log.info("Offset={}, Partition={}, Key={}, Value={}", consumerRecord.offset(),
                                consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                    }
                }
            }
        } catch (WakeupException e) {
                if (!closed.get()) {
                    throw e;
                }
            } finally {
                consumer.close();
            }
    }

    public void shutdown() {
        closed.set(false);
    }
}
