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
import com.example.redis_service.traceListener.producer.TraceProducer;

@Component
public class SetTtlOperationHandler implements RedisOperationHandler {

    private final StringRedisTemplate redisTemplate;
    private final KeyValueRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TraceProducer traceProducer;

    public SetTtlOperationHandler(StringRedisTemplate redisTemplate,
            KeyValueRepository repository,
            KafkaTemplate<String, Object> kafkaTemplate,
            TraceProducer traceProducer) {
        this.redisTemplate = redisTemplate;
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.traceProducer = traceProducer;
    }

    @Override
    public String getOperationType() {
        return "SET_TTL";
    }

    @Override
    public void handle(RedisEvent event) {
        try {
            traceProducer.sendTrace("SET_TTL operation: key=" + event.getKey());

            int ttlSeconds;
            try {
                ttlSeconds = Integer.parseInt(event.getValue());
                if (ttlSeconds <= 0 || ttlSeconds > 10) {
                    sendResponse(event, "FAILED", "TTL must be between 1 and 10 seconds");
                    return;
                }
            } catch (NumberFormatException e) {
                sendResponse(event, "FAILED", "Invalid TTL value: " + event.getValue());
                return;
            }

            String value = redisTemplate.opsForValue().get(event.getKey());

            if (value != null) {
                redisTemplate.expire(event.getKey(), ttlSeconds, TimeUnit.SECONDS);
                traceProducer.sendTrace("SET_TTL operation: TTL set in Redis");
                sendResponse(event, "SUCCESS", "TTL of " + ttlSeconds + "s set for key '" + event.getKey() + "'");
                return;
            }

            traceProducer.sendTrace("SET_TTL operation: Redis miss, checking Mongo");

            repository.findByKey(event.getKey()).ifPresentOrElse(doc -> {
                redisTemplate.opsForValue().set(event.getKey(), doc.getValue());
                redisTemplate.expire(event.getKey(), ttlSeconds, TimeUnit.SECONDS);
                traceProducer.sendTrace("SET_TTL operation: loaded from Mongo and TTL set");
                sendResponse(event, "SUCCESS", "Loaded from Mongo and TTL of " + ttlSeconds + "s set");
            }, () -> {
                traceProducer.sendTrace("SET_TTL operation: key not found");
                sendResponse(event, "NOT_FOUND", "Key '" + event.getKey() + "' not found in Redis or Mongo");
            });

        } catch (Exception e) {
            traceProducer.sendTrace("SET_TTL operation failed: " + e.getMessage());
            e.printStackTrace();
            sendResponse(event, "ERROR", "Failed to set TTL: " + e.getMessage());
        }
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