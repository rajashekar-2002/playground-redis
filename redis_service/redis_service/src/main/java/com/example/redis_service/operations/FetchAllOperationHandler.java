
package com.example.redis_service.operations;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
import com.example.redis_service.traceListener.producer.TraceProducer;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component
public class FetchAllOperationHandler implements RedisOperationHandler {

    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TraceProducer traceProducer;

    public FetchAllOperationHandler(StringRedisTemplate redisTemplate,
            KafkaTemplate<String, Object> kafkaTemplate, TraceProducer traceProducer) {
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.traceProducer = traceProducer;
    }

    @Override
    public String getOperationType() {
        return "FETCH_ALL";
    }

    @Override
    public void handle(RedisEvent event) {

        traceProducer.sendTrace("Fetching all keys");
        Set<String> keys = redisTemplate.keys("*");

        Map<String, String> result = new HashMap<>();

        if (keys != null) {
            for (String key : keys) {
                String value = redisTemplate.opsForValue().get(key);
                result.put(key, value);
            }
        }

        RedisResponseEvent response = RedisResponseEvent.builder()
                .uuid(event.getUuid())
                .operation(event.getOperation())
                .status("SUCCESSFULLY FETCHED ALL KEYS FROM REDIS")
                .message("Fetched all keys from Redis")
                .completedAt(LocalDateTime.now())
                .data(result)
                .build();

        kafkaTemplate.send("redis_response_topic", response);
        traceProducer.sendTrace("Sent response to Kafka");
    }
}