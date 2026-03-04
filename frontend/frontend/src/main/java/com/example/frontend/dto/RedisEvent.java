package com.example.frontend.dto;

import lombok.*;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RedisEvent {

    private String uuid;
    private OperationType operation;
    private String key;
    private String value;
    private LocalDateTime timestamp;
    private String status; // INITIATED, SUCCESS, FAILED
}