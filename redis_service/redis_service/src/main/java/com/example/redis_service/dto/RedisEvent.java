package com.example.redis_service.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RedisEvent {

    private String uuid;
    private String operation; // ADD, DELETE, UPDATE, FETCH_ALL
    private String key;
    private String value;
    private LocalDateTime timestamp;
    private String status; // INITIATED, SUCCESS, FAILED
}