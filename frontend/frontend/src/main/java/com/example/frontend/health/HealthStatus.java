package com.example.frontend.health;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HealthStatus {
    private boolean kafkaUp;
    private boolean redisUp;
    private boolean rabbitUp;
    private boolean mongoUp;

    // serviceName -> "UP" or "DOWN"
    private Map<String, String> serviceInstances;
}