package com.example.redis_service.task;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.operations.RedisOperationDispatcher;
import com.rabbitmq.client.Channel;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class RedisTaskConsumer {

    private final RedisOperationDispatcher dispatcher;
    private final RabbitTemplate rabbitTemplate;

    public RedisTaskConsumer(RedisOperationDispatcher dispatcher,
            RabbitTemplate rabbitTemplate) {
        this.dispatcher = dispatcher;
        this.rabbitTemplate = rabbitTemplate;
    }

    @RabbitListener(queues = "redis-queue", containerFactory = "rabbitListenerContainerFactory")
    public void handleRedisTask(RedisEvent event,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long tag) {

        try {

            dispatcher.dispatch(event);

            // if (!"FETCH_ALL".equals(event.getOperation())) {
            // rabbitTemplate.convertAndSend("mongo-queue", event);
            // }

            channel.basicAck(tag, false);

        } catch (Exception e) {

            try {
                channel.basicNack(tag, false, true);
            } catch (Exception nackEx) {
                nackEx.printStackTrace();
            }
        }
    }
}