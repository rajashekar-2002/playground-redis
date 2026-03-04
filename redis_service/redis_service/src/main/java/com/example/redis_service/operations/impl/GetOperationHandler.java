package com.example.redis_service.operations.impl;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
import com.example.redis_service.operations.RedisOperationHandler;
import com.example.redis_service.repository.KeyValueRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class GetOperationHandler implements RedisOperationHandler {

    private final StringRedisTemplate redisTemplate;
    private final KeyValueRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public GetOperationHandler(StringRedisTemplate redisTemplate,
            KeyValueRepository repository,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.redisTemplate = redisTemplate;
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String getOperationType() {
        return "GET";
    }

    @Override
    public void handle(RedisEvent event) {

        String value = redisTemplate.opsForValue().get(event.getKey());

        if (value != null) {
            sendResponse(event, value, "FOUND_IN_REDIS");
            return;
        }

        // 🔥 Redis Miss → check Mongo
        repository.findByKey(event.getKey())
                .ifPresentOrElse(doc -> {

                    // refill Redis
                    redisTemplate.opsForValue()
                            .set(event.getKey(), doc.getValue());

                    sendResponse(event, doc.getValue(), "FOUND_IN_MONGO");

                }, () -> {
                    sendResponse(event, null, "NOT_FOUND");
                });
    }

    private void sendResponse(RedisEvent event, String value, String status) {

        RedisResponseEvent response = RedisResponseEvent.builder()
                .uuid(event.getUuid())
                .key(event.getKey())
                .operation(event.getOperation())
                .status(status)
                .message(value)
                .completedAt(LocalDateTime.now())
                .build();

        kafkaTemplate.send("redis_response_topic", response);
    }
}