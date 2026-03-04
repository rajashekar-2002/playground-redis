package com.example.redis_service.operations;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
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

    public FetchAllOperationHandler(StringRedisTemplate redisTemplate,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String getOperationType() {
        return "FETCH_ALL";
    }

    @Override
    public void handle(RedisEvent event) {

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
                .status("SUCCESS")
                .message("Fetched all keys")
                .completedAt(LocalDateTime.now())
                .data(result)
                .build();

        kafkaTemplate.send("redis_response_topic", response);
    }
}