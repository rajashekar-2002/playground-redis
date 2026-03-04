package com.example.frontend.TraceListener.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import com.example.frontend.TraceListener.dto.TraceMessage;
import com.example.frontend.health.ServiceRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class TraceConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SimpMessagingTemplate messagingTemplate;
    private final ServiceRegistry serviceRegistry;

    public TraceConsumer(SimpMessagingTemplate messagingTemplate,
                         ServiceRegistry serviceRegistry) {
        this.messagingTemplate = messagingTemplate;
        this.serviceRegistry = serviceRegistry;
    }

    @KafkaListener(
        topics = "trace-response-topic",
        groupId = "frontend-trace-group",
        containerFactory = "traceKafkaListenerContainerFactory"
    )
    public void consumeTraceResponse(String jsonMessage) {
        try {
            // Strip outer quotes if double-serialized
            String cleaned = jsonMessage;
            if (jsonMessage.startsWith("\"") && jsonMessage.endsWith("\"")) {
                cleaned = objectMapper.readValue(jsonMessage, String.class);
            }

            TraceMessage message = objectMapper.readValue(cleaned, TraceMessage.class);

            // Register this service instance
            serviceRegistry.register(message.getServiceName());

            System.out.println("✅ Received trace response: " + message);
            messagingTemplate.convertAndSend("/topic/trace-updates", message);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}