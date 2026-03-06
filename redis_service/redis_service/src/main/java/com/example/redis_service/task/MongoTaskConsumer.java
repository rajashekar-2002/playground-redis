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
            KafkaTemplate<String, Object> kafkaTemplate,
            TraceProducer traceProducer) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.traceProducer = traceProducer;
    }

    @RabbitListener(queues = "mongo-queue", containerFactory = "rabbitListenerContainerFactory")
    public void handleMongoTask(RedisEvent event,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long tag) {

        try {
            String operation = event.getOperation();
            traceProducer.sendTrace("Received from RabbitMQ : Mongo worker queue | op=" + operation);

            // Only ADD and UPDATE reach this queue (enforced in RedisServiceListener).
            // Guard here as a safety net.
            if (!"ADD".equalsIgnoreCase(operation) && !"UPDATE".equalsIgnoreCase(operation)) {
                traceProducer.sendTrace("Ignoring unsupported operation in Mongo: " + operation);
                channel.basicAck(tag, false);
                return;
            }

            KeyValueDocument existing = repository.findByKey(event.getKey()).orElse(null);

            if (existing != null) {
                // UPDATE — key already exists in Mongo
                existing.setValue(event.getValue());
                existing.setUpdatedAt(LocalDateTime.now());
                repository.save(existing);

                sendResponse(event,
                        "UPDATED_IN_MONGO",
                        operation + " operation: Key '" + event.getKey() + "' updated in MongoDB");

            } else {
                // ADD (or UPDATE where key is new) — insert fresh document
                KeyValueDocument newDoc = new KeyValueDocument();
                newDoc.setId(event.getUuid());
                newDoc.setKey(event.getKey());
                newDoc.setValue(event.getValue());
                newDoc.setCreatedAt(event.getTimestamp());
                newDoc.setUpdatedAt(LocalDateTime.now());
                repository.save(newDoc);

                sendResponse(event,
                        "CREATED_IN_MONGO",
                        operation + " operation: Key '" + event.getKey() + "' created in MongoDB");
            }

            channel.basicAck(tag, false);
            traceProducer.sendTrace("Acked from Mongo worker queue");

        } catch (Exception e) {
            traceProducer.sendTrace("Error in Mongo worker queue");
            e.printStackTrace();
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
                event.getOperation(),
                status,
                message,
                LocalDateTime.now());

        kafkaTemplate.send("redis_response_topic", response);
        traceProducer.sendTrace("Sent to Kafka : Redis response topic | status=" + status);
    }
}