
package com.example.redis_service.operations.impl;

import java.time.LocalDateTime;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.dto.RedisResponseEvent;
import com.example.redis_service.operations.KafkaControlService;
import com.example.redis_service.operations.RedisOperationHandler;

@Component
public class PauseKafkaOperationHandler implements RedisOperationHandler {

    private final KafkaControlService kafkaControlService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PauseKafkaOperationHandler(KafkaControlService kafkaControlService,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaControlService = kafkaControlService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String getOperationType() {
        return "PAUSE_KAFKA";
    }

    @Override
    public void handle(RedisEvent event) {

        int seconds;

        try {
            seconds = Integer.parseInt(event.getValue());

            if (seconds <= 0 || seconds > 10) {
                sendResponse(event, "FAILED", "Pause must be 1-10 seconds");
                return;
            }

        } catch (Exception e) {
            sendResponse(event, "FAILED", "Invalid pause value");
            return;
        }

        kafkaControlService.pauseListener("redisListener", seconds);

        sendResponse(event, "SUCCESS",
                "Kafka paused for " + seconds + " seconds");
    }

    private void sendResponse(RedisEvent event, String status, String message) {

        RedisResponseEvent response = RedisResponseEvent.builder()
                .uuid(event.getUuid())
                .operation(event.getOperation())
                .status(status)
                .message(message)
                .completedAt(LocalDateTime.now())
                .build();

        kafkaTemplate.send("redis_response_topic", response);
    }
}