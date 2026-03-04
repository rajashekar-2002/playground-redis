package com.example.redis_service.listener;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.config.RabbitConfig;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class RedisServiceListener {

    private final RabbitTemplate rabbitTemplate;

    public RedisServiceListener(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    @KafkaListener(id = "redisListener", topics = "redis_opr_request_topic", groupId = "redis-service-group")
    public void listen(RedisEvent event) {

        System.out.println("Received from Kafka:-------------------------------------------- " + event);

        // Always send to Redis worker queue
        rabbitTemplate.convertAndSend(RabbitConfig.REDIS_QUEUE, event);

        // Only send to Mongo worker queue if operation is not DELETE

        String op = event.getOperation();
        if (!"DELETE".equalsIgnoreCase(op) && !"FETCH_ALL".equals(op) && !"SET_TTL".equals(op)) {
            rabbitTemplate.convertAndSend(RabbitConfig.MONGO_QUEUE, event); // use your configured queue
        }

    }
}