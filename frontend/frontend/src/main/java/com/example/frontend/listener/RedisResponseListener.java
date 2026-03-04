package com.example.frontend.listener;

import com.example.frontend.TraceListener.producer.TraceProducer;
import com.example.frontend.dto.RedisResponseEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class RedisResponseListener {

    private final SimpMessagingTemplate messagingTemplate;
    private final TraceProducer traceProducer;

    public RedisResponseListener(SimpMessagingTemplate messagingTemplate, TraceProducer traceProducer) {
        this.messagingTemplate = messagingTemplate;
        this.traceProducer = traceProducer;
    }

    @KafkaListener(topics = "redis_response_topic", containerFactory = "kafkaListenerContainerFactory")
    public void listen(RedisResponseEvent response) {
        traceProducer.sendTrace("kafka Listener received");

        messagingTemplate.convertAndSend(
                "/topic/kv-updates",
                response);

        traceProducer.sendTrace("WebSocket request received at /operate endpoint");

    }
}