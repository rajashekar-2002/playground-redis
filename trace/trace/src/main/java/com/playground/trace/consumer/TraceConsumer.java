package com.playground.trace.consumer;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.playground.trace.dto.TraceMessage;
import com.playground.trace.producer.TraceProducer;

@Service
public class TraceConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TraceConsumer.class);

    private final TraceProducer traceProducer;
    private final String serviceId = UUID.randomUUID().toString();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TraceConsumer(TraceProducer traceProducer) {
        this.traceProducer = traceProducer;
    }

    // Only ONE listener on trace-topic — TraceServiceHandler deleted
    @KafkaListener(topics = "trace-topic", groupId = "trace-service-group", containerFactory = "kafkaListenerContainerFactory")
    public void listenTrace(String jsonMessage) {
        try {
            TraceMessage message = objectMapper.readValue(jsonMessage, TraceMessage.class);
            logger.info("Received trace message: {}", message);

            message.setServiceName(message.getServiceName());
            message.setServiceId(serviceId);

            traceProducer.sendTraceResponse(message);
            logger.info("Sent trace response for messageId: {}", message.getMessageId());
        } catch (Exception e) {
            logger.error("Failed to process trace message: {}", jsonMessage, e);
        }
    }
}