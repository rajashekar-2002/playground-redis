package com.example.frontend.health;

import java.net.InetSocketAddress;
import java.net.Socket;

import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class HealthScheduler {

    private final SimpMessagingTemplate messagingTemplate;
    private final ServiceRegistry serviceRegistry;

    public HealthScheduler(SimpMessagingTemplate messagingTemplate,
                           ServiceRegistry serviceRegistry) {
        this.messagingTemplate = messagingTemplate;
        this.serviceRegistry = serviceRegistry;
    }

    @Scheduled(fixedRate = 5_000)
    public void pushHealth() {
        HealthStatus status = new HealthStatus(
            isReachable("localhost", 9092),   // Kafka
            isReachable("localhost", 6379),   // Redis
            isReachable("localhost", 5672),   // RabbitMQ
            isReachable("localhost", 27017),  // MongoDB  ← added
            serviceRegistry.getSnapshot()
        );
        messagingTemplate.convertAndSend("/topic/health-updates", status);
    }

    private boolean isReachable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1000);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}