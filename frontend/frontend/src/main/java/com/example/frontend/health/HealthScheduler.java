package com.example.frontend.health;

import java.net.InetSocketAddress;
import java.net.Socket;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class HealthScheduler {

    private final SimpMessagingTemplate messagingTemplate;
    private final ServiceRegistry serviceRegistry;

    @Value("${kafka.host:kafka}")
    private String kafkaHost;

    @Value("${spring.redis.host:redis}")
    private String redisHost;

    @Value("${spring.rabbitmq.host:rabbitmq}")
    private String rabbitmqHost;

    @Value("${spring.data.mongodb.host:mongodb}")
    private String mongoHost;

    public HealthScheduler(SimpMessagingTemplate messagingTemplate,
            ServiceRegistry serviceRegistry) {
        this.messagingTemplate = messagingTemplate;
        this.serviceRegistry = serviceRegistry;
    }

    @Scheduled(fixedRate = 5_000)
    public void pushHealth() {
        HealthStatus status = new HealthStatus(
                isReachable(kafkaHost, 9092),
                isReachable(redisHost, 6379),
                isReachable(rabbitmqHost, 5672),
                isReachable(mongoHost, 27017),
                serviceRegistry.getSnapshot());
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