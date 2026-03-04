package com.example.frontend.listener;

import com.example.frontend.dto.RedisResponseEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class RedisResponseListener {

    private final SimpMessagingTemplate messagingTemplate;

    public RedisResponseListener(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    @KafkaListener(topics = "redis_response_topic", groupId = "frontend-group-v2", containerFactory = "kafkaListenerContainerFactory")
    public void listen(RedisResponseEvent response) {

        System.out.println("Received response: --------------------------------------" + response);

        messagingTemplate.convertAndSend(
                "/topic/kv-updates",
                response);
    }
}