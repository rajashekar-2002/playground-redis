
package com.example.redis_service.operations.impl;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
import com.example.redis_service.operations.RedisOperationHandler;

import java.time.LocalDateTime;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class AddOperationHandler implements RedisOperationHandler {

    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate kafkaTemplate;

    public AddOperationHandler(StringRedisTemplate redisTemplate, KafkaTemplate kafkaTemplate) {
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String getOperationType() {
        return "ADD";
    }

    @Override
    public void handle(RedisEvent event) {
        redisTemplate.opsForValue()
                .set(event.getKey(), event.getValue());

        // send response to frontend
        RedisResponseEvent response = new RedisResponseEvent(
                event.getUuid(),
                event.getKey(),
                event.getOperation(),
                "SUCCESSFULLY ADDED IN REDIS",
                "KEY ADDED SUCCESSFULLY IN REDIS",
                LocalDateTime.now());

        kafkaTemplate.send("redis_response_topic", response);
    }
}