package com.kafka.apache;

import com.kafka.apache.callback.ProducerCallback;
import com.kafka.apache.consumers.ConsumerMessage;
import com.kafka.apache.multithread.ConsumerMultithread;
import com.kafka.apache.producers.ProducerMessage;
import com.kafka.apache.transactional.ProducerTransactional;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureAdapter;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootApplication
public class ApacheApplication implements CommandLineRunner {

	@Autowired
	private ProducerMessage producerMessage;
	@Autowired
	private ConsumerMessage consumerMessage;
	@Autowired
	private ConsumerMultithread consumerMultithread;
	@Autowired
	private ProducerTransactional producerTransactional;
	@Autowired
	private ProducerCallback producerCallback;
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	@Autowired
	private KafkaListenerEndpointRegistry registry;
	@Autowired
	private MeterRegistry meterRegistry;

	public static void main(String[] args) {
		SpringApplication.run(ApacheApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
//		Properties props = new Properties();
//		props.put("bootstrap.servers","localhost:9092");
//		props.put("acks","all");
//		props.put("compression.type","gzip");
//		props.put("linger.ms","1");
//		props.put("batch.size","32384");
//		props.put("transactional.id","devs4j-producer");
//		props.put("buffer.memory","33554432");
//		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//		producerMessage.produce();
//		consumerMessage.consume();

//		consumerMultithread.multiConsumer();

//		producerTransactional.produce();

//		producerCallback.produce();
//		ListenableFuture<SendResult<String, String>> future =
//				kafkaTemplate.send("devs4j-topic", "Sample message ");
//		future.addCallback(new KafkaSendCallback<String, String>() {
//
//			@Override
//			public void onSuccess(SendResult<String, String> result) {
//				log.info("Message sent {}", result.getRecordMetadata().offset());
//			}
//
//			@Override
//			public void onFailure(Throwable ex) {
//				KafkaSendCallback.super.onFailure(ex);
//				log.error("Error send message ", ex);
//			}
//
//			@Override
//			public void onFailure(KafkaProducerException e) {
//				log.error("Error send message ", e);
//			}
//		});

//		for (int i = 0; i < 100; i++) {
//			kafkaTemplate.send("devs4j-topic", String.valueOf(i), String.format("Sample message %d", i));
//		}

//		log.info("Waiting to start");
//		Thread.sleep(5000);
//		log.info("Starting");
//		registry.getListenerContainer("devs4j-id").start();
//		Thread.sleep(5000);
//		registry.getListenerContainer("devs4j-id").stop();
	}

	@KafkaListener(id = "devs4j-id", autoStartup = "true", topics = "devs4j-topic",
			containerFactory = "listenerContainerFactory", groupId = "devs4j-group",
			properties = {"max.poll.interval.ms:4000", "max.poll.records:50"})
	public void listen(List<ConsumerRecord<String, String>> messages) {
		log.info("Messages received: {}", messages.size());
//		log.info("Start reading messages");
		for (ConsumerRecord<String, String> message : messages) {
//			log.info("Offset={}, Partition={}, Key={}, Value={}", message.offset(),
//					message.partition(), message.key(), message.value());
		}
//		log.info("Batch completed");
	}

	@Scheduled(fixedDelay = 2000, initialDelay = 100)
	public void sendKafkaMessages() {
		for (int i = 0; i < 200; i++) {
			kafkaTemplate.send("devs4j-topic", String.valueOf(i), String.format("Sample message %d", i));
		}
	}

	@Scheduled(fixedDelay = 2000, initialDelay = 500)
	public void printMetrics() {
		List<Meter> metrics = meterRegistry.getMeters();
		for (Meter meter : metrics) {
			log.info("Meter={}", meter.getId().getName());
		}
		Double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
		log.info("Count {} ",count);
	}
}
