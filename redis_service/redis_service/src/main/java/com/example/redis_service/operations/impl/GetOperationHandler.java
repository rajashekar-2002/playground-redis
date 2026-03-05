package com.example.redis_service.operations.impl;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
import com.example.redis_service.operations.RedisOperationHandler;
import com.example.redis_service.repository.KeyValueRepository;
import com.example.redis_service.traceListener.producer.TraceProducer;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class GetOperationHandler implements RedisOperationHandler {

    private final StringRedisTemplate redisTemplate;
    private final KeyValueRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TraceProducer traceProducer;

    public GetOperationHandler(StringRedisTemplate redisTemplate,
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
        return "GET";
    }

    @Override
    public void handle(RedisEvent event) {
        try {
            traceProducer.sendTrace("GET operation: key=" + event.getKey());

            String redisValue = redisTemplate.opsForValue().get(event.getKey());

            if (redisValue != null) {
                traceProducer.sendTrace("GET operation: found in Redis");
                sendResponse(event, redisValue, "FOUND_IN_REDIS");
                return;
            }

            traceProducer.sendTrace("GET operation: Redis miss, checking Mongo");

            repository.findByKey(event.getKey())
                    .ifPresentOrElse(doc -> {
                        redisTemplate.opsForValue().set(event.getKey(), doc.getValue());
                        traceProducer.sendTrace("GET operation: found in Mongo, reloaded to Redis");
                        sendResponse(event, doc.getValue(), "FOUND_IN_MONGO");
                    }, () -> {
                        traceProducer.sendTrace("GET operation: not found anywhere");
                        sendResponse(event, null, "NOT_FOUND");
                    });

        } catch (Exception e) {
            traceProducer.sendTrace("GET operation failed: " + e.getMessage());
            e.printStackTrace();
            sendResponse(event, null, "ERROR: " + e.getMessage());
        }
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