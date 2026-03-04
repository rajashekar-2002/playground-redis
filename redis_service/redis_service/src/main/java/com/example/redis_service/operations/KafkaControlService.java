package com.example.redis_service.operations;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import com.example.redis_service.traceListener.producer.TraceProducer;

@Service
public class KafkaControlService {

    private final KafkaListenerEndpointRegistry registry;
    private final TraceProducer traceProducer;

    public KafkaControlService(KafkaListenerEndpointRegistry registry, TraceProducer traceProducer) {
        this.registry = registry;
        this.traceProducer = traceProducer;
    }

    public void pauseListener(String listenerId, int seconds) {
        traceProducer.sendTrace("Pausing Kafka listener for " + seconds + " seconds");

        MessageListenerContainer container = registry.getListenerContainer(listenerId);

        if (container == null) {
            traceProducer.sendTrace("Listener not found: " + listenerId);
            return;
        }

        if (container.isPauseRequested()) {
            traceProducer.sendTrace("Listener already paused");
            return;
        }

        traceProducer.sendTrace("Pausing Kafka listener...");
        container.pause();

        Executors.newSingleThreadScheduledExecutor()
                .schedule(() -> {
                    try {
                        traceProducer.sendTrace("Resuming Kafka listener...");
                        container.resume();
                    } catch (Exception e) {
                        traceProducer.sendTrace("Error resuming Kafka listener");
                        e.printStackTrace();
                    }
                }, seconds, TimeUnit.SECONDS);
    }
}