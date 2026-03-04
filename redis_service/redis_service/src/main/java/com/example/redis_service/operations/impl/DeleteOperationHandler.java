package com.example.redis_service.operations.impl;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
import com.example.redis_service.operations.RedisOperationHandler;

import java.time.LocalDateTime;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class DeleteOperationHandler implements RedisOperationHandler {

    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate kafkaTemplate;

    public DeleteOperationHandler(StringRedisTemplate redisTemplate, KafkaTemplate kafkaTemplate) {
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String getOperationType() {
        return "DELETE";
    }

    @Override
    public void handle(RedisEvent event) {
        // Delete the key from Redis
        redisTemplate.delete(event.getKey());
        System.out.println("Deleted key from Redis: " + event.getKey());

        // send response to frontend
        RedisResponseEvent response = new RedisResponseEvent(
                event.getUuid(),
                event.getKey(),
                event.getOperation(),
                "SUCCESS",
                "Key deleted successfully",
                LocalDateTime.now());

        kafkaTemplate.send("redis_response_topic", response);
    }
}