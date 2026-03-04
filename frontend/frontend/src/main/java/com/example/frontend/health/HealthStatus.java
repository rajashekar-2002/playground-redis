package com.example.frontend.health;

import java.util.List;
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
    private boolean mongoUp;                        // ← added
    private Map<String, List<String>> serviceInstances;
}