package com.example.redis_service.traceListener;

import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class ServiceIdGenerator {
    private final String serviceId = UUID.randomUUID().toString();

    public String getServiceId() {
        return serviceId;
    }
}