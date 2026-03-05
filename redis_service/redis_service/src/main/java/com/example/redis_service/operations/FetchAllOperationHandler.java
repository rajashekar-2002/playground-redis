package com.example.redis_service.operations;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
import com.example.redis_service.traceListener.producer.TraceProducer;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

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

        try {
            traceProducer.sendTrace("Fetching all keys using SCAN");

            Map<String, String> result = new HashMap<>();

            // SCAN is non-blocking, iterates in small batches unlike keys("*")
            ScanOptions options = ScanOptions.scanOptions()
                    .count(100) // fetch 100 keys per iteration
                    .build();

            try (Cursor<String> cursor = redisTemplate.scan(options)) {
                while (cursor.hasNext()) {
                    String key = cursor.next();
                    try {
                        String value = redisTemplate.opsForValue().get(key);
                        result.put(key, value != null ? value : "");
                    } catch (Exception e) {
                        traceProducer.sendTrace("Error fetching value for key: " + key + " - " + e.getMessage());
                    }
                }
            }

            RedisResponseEvent response = RedisResponseEvent.builder()
                    .uuid(event.getUuid())
                    .operation(event.getOperation())
                    .status("SUCCESS")
                    .message("Fetched " + result.size() + " keys from Redis")
                    .completedAt(LocalDateTime.now())
                    .data(result)
                    .build();

            kafkaTemplate.send("redis_response_topic", response);
            traceProducer.sendTrace("Sent response to Kafka with " + result.size() + " keys");

        } catch (Exception e) {
            traceProducer.sendTrace("Error in FetchAllOperationHandler: " + e.getMessage());
            e.printStackTrace();

            RedisResponseEvent errorResponse = RedisResponseEvent.builder()
                    .uuid(event.getUuid())
                    .operation(event.getOperation())
                    .status("ERROR")
                    .message("Failed to fetch keys from Redis: " + e.getMessage())
                    .completedAt(LocalDateTime.now())
                    .data(new HashMap<>())
                    .build();

            kafkaTemplate.send("redis_response_topic", errorResponse);
        }
    }
}