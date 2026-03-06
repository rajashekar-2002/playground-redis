package com.example.redis_service.listener;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.traceListener.producer.TraceProducer;
import com.example.redis_service.config.RabbitConfig;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class RedisServiceListener {

    private final RabbitTemplate rabbitTemplate;
    private final TraceProducer traceProducer;

    public RedisServiceListener(RabbitTemplate rabbitTemplate, TraceProducer traceProducer) {
        this.rabbitTemplate = rabbitTemplate;
        this.traceProducer = traceProducer;
    }

    @KafkaListener(id = "redisListener", topics = "redis_opr_request_topic", containerFactory = "kafkaListenerContainerFactory")
    public void listen(RedisEvent event) {

        traceProducer.sendTrace("Received from Kafka topic redis_opr_request_topic");

        String op = event.getOperation();

        // GET, FETCH_ALL, DELETE, SET_TTL → Redis queue ONLY
        // ADD, UPDATE → both Redis and Mongo queues
        rabbitTemplate.convertAndSend(RabbitConfig.REDIS_QUEUE, event);
        traceProducer.sendTrace("Sent to RabbitMQ : Redis worker queue");

        if ("ADD".equalsIgnoreCase(op) || "UPDATE".equalsIgnoreCase(op)) {
            rabbitTemplate.convertAndSend(RabbitConfig.MONGO_QUEUE, event);
            traceProducer.sendTrace("Sent to RabbitMQ : Mongo worker queue");
        }
    }
}