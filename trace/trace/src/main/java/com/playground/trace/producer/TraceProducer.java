package com.playground.trace.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class TraceProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public TraceProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper();
    }

    public void sendTraceResponse(Object message) {
        try {
            String json = objectMapper.writeValueAsString(message);
            kafkaTemplate.send("trace-response-topic", json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}