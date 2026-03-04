package com.example.frontend.TraceListener.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class TraceProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public TraceProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper();
    }

    public void sendTrace(String payload, String serviceId) {
        try {
            Map<String, Object> message = new HashMap<>();
            message.put("serviceName", "frontend");
            message.put("serviceId", serviceId);
            message.put("messageId", UUID.randomUUID().toString());
            message.put("payload", payload);

            String json = objectMapper.writeValueAsString(message);
            kafkaTemplate.send("trace-topic", json);
            System.out.println("Sent trace message: " + json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}