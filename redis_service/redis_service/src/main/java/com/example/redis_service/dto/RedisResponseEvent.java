package com.example.redis_service.dto;

import java.time.LocalDateTime;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
@AllArgsConstructor
public class RedisResponseEvent {

    private String uuid;
    private String key;
    private String operation;
    private String status; // SUCCESS / FAILED
    private String message;
    private LocalDateTime completedAt;

    private Map<String, String> data; // ← for FETCH_ALL

    public RedisResponseEvent() {
    }

    public RedisResponseEvent(String uuid,
            String key,
            String operation,
            String status,
            String message,
            LocalDateTime completedAt) {
        this.uuid = uuid;
        this.key = key;
        this.operation = operation;
        this.status = status;
        this.message = message;
        this.completedAt = completedAt;
    }

    // getters + setters
}