package com.example.frontend.TraceListener.consumer;

import com.example.frontend.TraceListener.dto.TraceMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class TraceConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SimpMessagingTemplate messagingTemplate;

    public TraceConsumer(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    @KafkaListener(topics = "trace-response-topic", groupId = "frontend-trace-group", containerFactory = "traceKafkaListenerContainerFactory")
    public void consumeTraceResponse(String jsonMessage) {
        try {
            // Strip outer quotes if double-serialized (e.g. "\"{...}\"" → "{...}")
            String cleaned = jsonMessage;
            if (jsonMessage.startsWith("\"") && jsonMessage.endsWith("\"")) {
                cleaned = objectMapper.readValue(jsonMessage, String.class);
            }

            TraceMessage message = objectMapper.readValue(cleaned, TraceMessage.class);
            System.out.println("✅ Received trace response:OOOOOOOOOOOOOOOOOOOOOO " + message);
            messagingTemplate.convertAndSend("/topic/trace-updates", message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}