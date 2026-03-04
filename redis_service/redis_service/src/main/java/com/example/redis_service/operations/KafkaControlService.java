package com.example.redis_service.operations;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

@Service
public class KafkaControlService {

    private final KafkaListenerEndpointRegistry registry;

    public KafkaControlService(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    public void pauseListener(String listenerId, int seconds) {

        MessageListenerContainer container = registry.getListenerContainer(listenerId);

        if (container == null) {
            System.out.println("Listener not found: " + listenerId);
            return;
        }

        if (container.isPauseRequested()) {
            System.out.println("Listener already paused");
            return;
        }

        System.out.println("Pausing Kafka listener...");
        container.pause();

        Executors.newSingleThreadScheduledExecutor()
                .schedule(() -> {
                    try {
                        System.out.println("Resuming Kafka listener...");
                        container.resume();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, seconds, TimeUnit.SECONDS);
    }
}