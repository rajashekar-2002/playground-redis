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
public class DeleteOperationHandler implements RedisOperationHandler {

    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TraceProducer traceProducer;

    public DeleteOperationHandler(StringRedisTemplate redisTemplate,
            KafkaTemplate<String, Object> kafkaTemplate,
            TraceProducer traceProducer) {
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.traceProducer = traceProducer;
    }

    @Override
    public String getOperationType() {
        return "DELETE";
    }

    @Override
    public void handle(RedisEvent event) {
        try {
            traceProducer.sendTrace("DELETE operation: key=" + event.getKey());
            Boolean deleted = redisTemplate.delete(event.getKey());
            traceProducer.sendTrace("DELETE operation: deleted=" + deleted);

            RedisResponseEvent response = new RedisResponseEvent(
                    event.getUuid(),
                    event.getKey(),
                    event.getOperation(),
                    deleted != null && deleted ? "SUCCESS" : "NOT_FOUND",
                    deleted != null && deleted
                            ? "Key '" + event.getKey() + "' deleted from Redis"
                            : "Key '" + event.getKey() + "' not found in Redis",
                    LocalDateTime.now());

            kafkaTemplate.send("redis_response_topic", response);
            traceProducer.sendTrace("DELETE operation: response sent to Kafka");

        } catch (Exception e) {
            traceProducer.sendTrace("DELETE operation failed: " + e.getMessage());
            e.printStackTrace();

            kafkaTemplate.send("redis_response_topic", new RedisResponseEvent(
                    event.getUuid(), event.getKey(), event.getOperation(),
                    "ERROR", "Failed to delete key: " + e.getMessage(), LocalDateTime.now()));
        }
    }
}