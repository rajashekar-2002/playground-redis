package com.example.redis_service.operations.impl;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
import com.example.redis_service.operations.RedisOperationHandler;
import com.example.redis_service.traceListener.producer.TraceProducer;

import java.time.LocalDateTime;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class AddOperationHandler implements RedisOperationHandler {

    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TraceProducer traceProducer;

    public AddOperationHandler(StringRedisTemplate redisTemplate,
            KafkaTemplate<String, Object> kafkaTemplate,
            TraceProducer traceProducer) {
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.traceProducer = traceProducer;
    }

    @Override
    public String getOperationType() {
        return "ADD";
    }

    @Override
    public void handle(RedisEvent event) {
        try {
            traceProducer.sendTrace("ADD operation: key=" + event.getKey());
            redisTemplate.opsForValue().set(event.getKey(), event.getValue());
            traceProducer.sendTrace("ADD operation: stored in Redis");

            RedisResponseEvent response = new RedisResponseEvent(
                    event.getUuid(),
                    event.getKey(),
                    event.getOperation(),
                    "SUCCESS",
                    "Key '" + event.getKey() + "' added successfully in Redis",
                    LocalDateTime.now());

            kafkaTemplate.send("redis_response_topic", response);
            traceProducer.sendTrace("ADD operation: response sent to Kafka");

        } catch (Exception e) {
            traceProducer.sendTrace("ADD operation failed: " + e.getMessage());
            e.printStackTrace();
            sendErrorResponse(event, e.getMessage());
        }
    }

    private void sendErrorResponse(RedisEvent event, String errorMsg) {
        RedisResponseEvent errorResponse = new RedisResponseEvent(
                event.getUuid(),
                event.getKey(),
                event.getOperation(),
                "ERROR",
                "Failed to add key: " + errorMsg,
                LocalDateTime.now());
        kafkaTemplate.send("redis_response_topic", errorResponse);
    }
}