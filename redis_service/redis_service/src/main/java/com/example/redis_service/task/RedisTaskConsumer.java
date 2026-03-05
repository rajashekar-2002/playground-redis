package com.example.redis_service.task;

import com.example.redis_service.dto.RedisEvent;
import com.example.redis_service.operations.RedisOperationDispatcher;
import com.example.redis_service.traceListener.producer.TraceProducer;
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
    private final TraceProducer traceProducer;

    public RedisTaskConsumer(RedisOperationDispatcher dispatcher,
            RabbitTemplate rabbitTemplate, TraceProducer traceProducer) {
        this.dispatcher = dispatcher;
        this.rabbitTemplate = rabbitTemplate;
        this.traceProducer = traceProducer;
    }

    @RabbitListener(queues = "redis-queue", containerFactory = "rabbitListenerContainerFactory")
    public void handleRedisTask(RedisEvent event,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long tag) {

        try {
            traceProducer.sendTrace("Received from RabbitMQ : Redis worker queue");
            dispatcher.dispatch(event);
            traceProducer.sendTrace("Dispatched operation: " + event.getOperation());
            channel.basicAck(tag, false);
            traceProducer.sendTrace("Acked from Redis worker queue");

        } catch (Exception e) {
            traceProducer.sendTrace("Error in Redis worker queue: " + e.getMessage());
            e.printStackTrace(); // always print so we can see root cause
            try {
                channel.basicNack(tag, false, false); // false = don't requeue, prevents infinite loop
            } catch (Exception nackEx) {
                nackEx.printStackTrace();
            }
        }
    }
}