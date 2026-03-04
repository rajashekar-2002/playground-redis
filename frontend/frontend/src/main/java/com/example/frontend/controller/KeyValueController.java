package com.example.frontend.controller;

import com.example.frontend.TraceListener.producer.TraceProducer;
import com.example.frontend.dto.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

import java.time.LocalDateTime;
import java.util.UUID;

@Controller
public class KeyValueController {

    private final KafkaTemplate<String, RedisEvent> kafkaTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final TraceProducer traceProducer;

    private static final String TOPIC = "redis_opr_request_topic";

    public KeyValueController(KafkaTemplate<String, RedisEvent> kafkaTemplate,
            SimpMessagingTemplate messagingTemplate, TraceProducer traceProducer) {
        this.kafkaTemplate = kafkaTemplate;
        this.messagingTemplate = messagingTemplate;
        this.traceProducer = traceProducer;
    }

    @MessageMapping("/operate")
    public void handleOperation(KeyValueMessage msg) {

        RedisEvent event = RedisEvent.builder()
                .uuid(UUID.randomUUID().toString())
                .operation(msg.getOperation())
                .key(msg.getKey())
                .value(msg.getValue())
                .timestamp(LocalDateTime.now())
                .status("INITIATED")
                .build();

        kafkaTemplate.send(TOPIC, event);

        traceProducer.sendTrace(TOPIC, event.getUuid());

        // Immediately notify frontend UI
        messagingTemplate.convertAndSend("/topic/kv-updates", event);

        System.out.println("Sent to Kafka: ------------------------------------" + event);
    }
}