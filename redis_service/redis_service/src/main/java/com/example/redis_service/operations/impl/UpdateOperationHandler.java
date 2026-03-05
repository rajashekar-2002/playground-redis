package com.example.redis_service.operations.impl;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
import com.example.redis_service.operations.RedisOperationHandler;
import com.example.redis_service.traceListener.producer.TraceProducer;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class UpdateOperationHandler implements RedisOperationHandler {

    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TraceProducer traceProducer;

    public UpdateOperationHandler(StringRedisTemplate redisTemplate,
            KafkaTemplate<String, Object> kafkaTemplate,
            TraceProducer traceProducer) {
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.traceProducer = traceProducer;
    }

    @Override
    public String getOperationType() {
        return "UPDATE";
    }

    @Override
    public void handle(RedisEvent event) {
        try {
            traceProducer.sendTrace("UPDATE operation: key=" + event.getKey());

            Boolean exists = redisTemplate.hasKey(event.getKey());

            if (exists == null || !exists) {
                traceProducer.sendTrace("UPDATE operation: key not found");
                kafkaTemplate.send("redis_response_topic", RedisResponseEvent.builder()
                        .uuid(event.getUuid())
                        .key(event.getKey())
                        .operation(event.getOperation())
                        .status("NOT_FOUND")
                        .message("Key '" + event.getKey() + "' not found in Redis")
                        .completedAt(LocalDateTime.now())
                        .build());
                return;
            }

            redisTemplate.opsForValue().set(event.getKey(), event.getValue());
            traceProducer.sendTrace("UPDATE operation: updated in Redis");

            kafkaTemplate.send("redis_response_topic", RedisResponseEvent.builder()
                    .uuid(event.getUuid())
                    .key(event.getKey())
                    .operation(event.getOperation())
                    .status("SUCCESS")
                    .message("Key '" + event.getKey() + "' updated successfully in Redis")
                    .completedAt(LocalDateTime.now())
                    .build());

            traceProducer.sendTrace("UPDATE operation: response sent to Kafka");

        } catch (Exception e) {
            traceProducer.sendTrace("UPDATE operation failed: " + e.getMessage());
            e.printStackTrace();

            kafkaTemplate.send("redis_response_topic", RedisResponseEvent.builder()
                    .uuid(event.getUuid())
                    .key(event.getKey())
                    .operation(event.getOperation())
                    .status("ERROR")
                    .message("Failed to update key: " + e.getMessage())
                    .completedAt(LocalDateTime.now())
                    .build());
        }
    }
}