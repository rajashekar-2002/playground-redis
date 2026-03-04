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

@Service
public class MongoTaskConsumer {

    private final KeyValueRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public MongoTaskConsumer(KeyValueRepository repository,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @RabbitListener(queues = "mongo-queue", containerFactory = "rabbitListenerContainerFactory")
    public void handleMongoTask(RedisEvent event,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long tag) {

        try {

            // Check if a document with the same key already exists
            KeyValueDocument doc = repository.findByKey(event.getKey())
                    .orElse(null);

            if (doc != null) {
                // UPDATE existing
                doc.setValue(event.getValue());
                doc.setOperation(event.getOperation());
                doc.setStatus("SUCCESS");
                doc.setUpdatedAt(LocalDateTime.now());

            } else {
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

            // ACK
            channel.basicAck(tag, false);

        } catch (Exception e) {
            e.printStackTrace();
            try {
                channel.basicNack(tag, false, true); // retry
            } catch (Exception nackEx) {
                nackEx.printStackTrace();
            }
        }
    }
}