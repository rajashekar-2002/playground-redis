
package com.example.redis_service.operations.impl;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
import com.example.redis_service.operations.RedisOperationHandler;
import com.example.redis_service.repository.KeyValueRepository;

@Component
public class SetTtlOperationHandler implements RedisOperationHandler {

    private final StringRedisTemplate redisTemplate;
    private final KeyValueRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public SetTtlOperationHandler(StringRedisTemplate redisTemplate,
            KeyValueRepository repository,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.redisTemplate = redisTemplate;
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String getOperationType() {
        return "SET_TTL";
    }

    @Override
    public void handle(RedisEvent event) {

        String key = event.getKey();
        String ttlValue = event.getValue();

        int ttlSeconds;

        try {
            ttlSeconds = Integer.parseInt(ttlValue);

            if (ttlSeconds <= 0 || ttlSeconds > 10) {
                sendResponse(event, "FAILED", "TTL must be between 1 and 10 seconds");
                return;
            }

        } catch (Exception e) {
            sendResponse(event, "FAILED", "Invalid TTL value");
            return;
        }

        // 1️⃣ Check Redis
        String value = redisTemplate.opsForValue().get(key);

        if (value != null) {

            redisTemplate.expire(key, ttlSeconds, TimeUnit.SECONDS);
            sendResponse(event, "SUCCESS", "TTL updated in Redis");
            return;
        }

        // 2️⃣ Redis miss → check Mongo
        repository.findByKey(key).ifPresentOrElse(doc -> {

            redisTemplate.opsForValue().set(key, doc.getValue());
            redisTemplate.expire(key, ttlSeconds, TimeUnit.SECONDS);

            sendResponse(event, "SUCCESS", "Loaded from Mongo and TTL set");

        }, () -> {
            sendResponse(event, "NOT_FOUND", "Key not present in Redis or Mongo");
        });
    }

    private void sendResponse(RedisEvent event, String status, String message) {

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