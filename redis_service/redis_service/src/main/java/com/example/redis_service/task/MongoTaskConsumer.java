package com.example.redis_service.task;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
import com.example.redis_service.models.KeyValueDocument;
import com.example.redis_service.repository.KeyValueRepository;
import com.example.redis_service.traceListener.producer.TraceProducer;
import com.rabbitmq.client.Channel;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

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

    // @RabbitListener(queues = "mongo-queue", containerFactory =
    // "rabbitListenerContainerFactory")
    // public void handleMongoTask(RedisEvent event,
    // Channel channel,
    // @Header(AmqpHeaders.DELIVERY_TAG) long tag) {

    // try {
    // traceProducer.sendTrace("Received from RabbitMQ : Mongo worker queue");
    // // Check if a document with the same key already exists
    // KeyValueDocument doc = repository.findByKey(event.getKey())
    // .orElse(null);

    // if (doc != null) {
    // traceProducer.sendTrace("Updating existing document");
    // // UPDATE existing
    // doc.setValue(event.getValue());
    // doc.setOperation(event.getOperation());
    // doc.setStatus("SUCCESS");
    // doc.setUpdatedAt(LocalDateTime.now());

    // } else {
    // traceProducer.sendTrace("Creating new document");
    // // CREATE new
    // doc = new KeyValueDocument();
    // doc.setId(event.getUuid()); // only set id for new document
    // doc.setKey(event.getKey());
    // doc.setValue(event.getValue());
    // doc.setOperation(event.getOperation());
    // doc.setStatus("SUCCESS");
    // doc.setCreatedAt(event.getTimestamp());
    // doc.setUpdatedAt(LocalDateTime.now());
    // }

    // repository.save(doc); // insert or update

    // // Send FINAL response to frontend
    // RedisResponseEvent response = new RedisResponseEvent(
    // event.getUuid(),
    // event.getKey(),
    // event.getOperation(),
    // "SUCCESS",
    // "Operation completed successfully",
    // LocalDateTime.now());

    // kafkaTemplate.send("redis_response_topic", response);
    // traceProducer.sendTrace("Sent to Kafka : Redis response topic");
    // // ACK
    // channel.basicAck(tag, false);
    // traceProducer.sendTrace("Acked from Mongo worker queue");

    // } catch (Exception e) {
    // traceProducer.sendTrace("Error in Mongo worker queue");
    // e.printStackTrace();
    // try {
    // channel.basicNack(tag, false, true); // retry
    // } catch (Exception nackEx) {
    // nackEx.printStackTrace();
    // }
    // }
    // }

    @RabbitListener(queues = "mongo-queue", containerFactory = "rabbitListenerContainerFactory")
    public void handleMongoTask(RedisEvent event,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long tag) {

        try {

            String operation = event.getOperation();
            traceProducer.sendTrace("Received from RabbitMQ : Mongo worker queue");

            // Ignore read-only operations
            if ("GET".equalsIgnoreCase(operation)
                    || "FETCH_ALL".equalsIgnoreCase(operation)
                    || "SET_TTL".equalsIgnoreCase(operation)) {

                traceProducer.sendTrace("Ignoring read-only operation in Mongo");
                channel.basicAck(tag, false);
                return;
            }

            // ---------------- DELETE ----------------
            if ("DELETE".equalsIgnoreCase(operation)) {

                boolean exists = repository.findByKey(event.getKey()).isPresent();

                if (exists) {
                    repository.deleteByKey(event.getKey());

                    sendResponse(event,
                            "SUCCESS",
                            "DELETE operation: Key '" + event.getKey() + "' deleted from MongoDB");

                } else {

                    sendResponse(event,
                            "NOT_FOUND",
                            "DELETE operation: Key '" + event.getKey() + "' not found in MongoDB");
                }

                channel.basicAck(tag, false);
                return;
            }

            // ---------------- UPDATE / SET ----------------
            KeyValueDocument existingDoc = repository.findByKey(event.getKey())
                    .orElse(null);

            if (existingDoc != null) {

                existingDoc.setValue(event.getValue());
                existingDoc.setUpdatedAt(LocalDateTime.now());
                repository.save(existingDoc);

                sendResponse(event,
                        "UPDATED",
                        operation + " operation: Existing key '" + event.getKey()
                                + "' updated in MongoDB");

            } else {

                KeyValueDocument newDoc = new KeyValueDocument();
                newDoc.setId(event.getUuid());
                newDoc.setKey(event.getKey());
                newDoc.setValue(event.getValue());
                newDoc.setCreatedAt(event.getTimestamp());
                newDoc.setUpdatedAt(LocalDateTime.now());

                repository.save(newDoc);

                sendResponse(event,
                        "CREATED",
                        operation + " operation: New key '" + event.getKey()
                                + "' created in MongoDB");
            }

            channel.basicAck(tag, false);

        } catch (Exception e) {

            traceProducer.sendTrace("Error in Mongo worker queue");

            try {
                channel.basicNack(tag, false, true);
            } catch (Exception nackEx) {
                nackEx.printStackTrace();
            }
        }
    }

    private void sendResponse(RedisEvent event, String status, String message) {

        RedisResponseEvent response = new RedisResponseEvent(
                event.getUuid(),
                event.getKey(),
                event.getOperation(), // operation explicitly included
                status,
                message,
                LocalDateTime.now());

        kafkaTemplate.send("redis_response_topic", response);
    }
}