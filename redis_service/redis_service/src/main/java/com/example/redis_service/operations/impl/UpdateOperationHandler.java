package com.example.redis_service.operations.impl;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
import com.example.redis_service.operations.RedisOperationHandler;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class UpdateOperationHandler implements RedisOperationHandler {

    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public UpdateOperationHandler(StringRedisTemplate redisTemplate,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String getOperationType() {
        return "UPDATE";
    }

    @Override
    public void handle(RedisEvent event) {

        boolean exists = redisTemplate.hasKey(event.getKey());

        // Update if present, insert if not — Redis always gets the latest value
        redisTemplate.opsForValue().set(event.getKey(), event.getValue());

        String status = exists ? "UPDATED_IN_REDIS" : "CREATED_IN_REDIS";
        String message = exists
                ? "Key '" + event.getKey() + "' updated in Redis"
                : "Key '" + event.getKey() + "' not found in Redis — inserted as new";

        RedisResponseEvent response = RedisResponseEvent.builder()
                .uuid(event.getUuid())
                .key(event.getKey())
                .operation(event.getOperation())
                .status(status)
                .message(message)
                .completedAt(LocalDateTime.now())
                .build();

        kafkaTemplate.send("redis_response_topic", response);
    }
}