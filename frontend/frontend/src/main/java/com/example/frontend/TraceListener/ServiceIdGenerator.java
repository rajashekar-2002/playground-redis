package com.example.frontend.TraceListener;

import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class ServiceIdGenerator {
    private final String serviceId = UUID.randomUUID().toString();

    public String getServiceId() {
        return serviceId;
    }
}