package com.example.redis_service.task;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
import com.example.redis_service.models.KeyValueDocument;
import com.example.redis_service.repository.KeyValueRepository;
import com.rabbitmq.client.Channel;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import com.example.redis_service.TraceListener.producer.TraceProducer;

@Service
public class MongoTaskConsumer {

    private final KeyValueRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TraceProducer traceProducer;

    public MongoTaskConsumer(KeyValueRepository repository,
            KafkaTemplate<String, Object> kafkaTemplate, TraceProducer traceProducer) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.traceProducer = traceProducer;
    }

    @RabbitListener(queues = "mongo-queue", containerFactory = "rabbitListenerContainerFactory")
    public void handleMongoTask(RedisEvent event,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long tag) {

        try {
            traceProducer.sendTrace("Received from RabbitMQ : Mongo worker queue");
            // Check if a document with the same key already exists
            KeyValueDocument doc = repository.findByKey(event.getKey())
                    .orElse(null);

            if (doc != null) {
                traceProducer.sendTrace("Updating existing document");
                // UPDATE existing
                doc.setValue(event.getValue());
                doc.setOperation(event.getOperation());
                doc.setStatus("SUCCESS");
                doc.setUpdatedAt(LocalDateTime.now());

            } else {
                traceProducer.sendTrace("Creating new document");
                // CREATE new
                doc = new KeyValueDocument();
                doc.setId(event.getUuid()); // only set id for new document
                doc.setKey(event.getKey());
                doc.setValue(event.getValue());
                doc.setOperation(event.getOperation());
                doc.setStatus("SUCCESS");
                doc.setCreatedAt(event.getTimestamp());
                doc.setUpdatedAt(LocalDateTime.now());
            }

            repository.save(doc); // insert or update

            // Send FINAL response to frontend
            RedisResponseEvent response = new RedisResponseEvent(
                    event.getUuid(),
                    event.getKey(),
                    event.getOperation(),
                    "SUCCESS",
                    "Operation completed successfully",
                    LocalDateTime.now());

            kafkaTemplate.send("redis_response_topic", response);
            traceProducer.sendTrace("Sent to Kafka : Redis response topic");
            // ACK
            channel.basicAck(tag, false);
            traceProducer.sendTrace("Acked from Mongo worker queue");

        } catch (Exception e) {
            traceProducer.sendTrace("Error in Mongo worker queue");
            e.printStackTrace();
            try {
                channel.basicNack(tag, false, true); // retry
            } catch (Exception nackEx) {
                nackEx.printStackTrace();
            }
        }
    }
}