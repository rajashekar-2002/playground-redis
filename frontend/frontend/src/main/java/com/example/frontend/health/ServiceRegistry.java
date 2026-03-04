package com.example.frontend.health;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class ServiceRegistry {

    private static final long STALE_THRESHOLD_MS = 90_000; // 90s = 3 missed heartbeats

    // serviceName -> lastSeenTimestamp (any instance counts)
    private final Map<String, Long> registry = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void register(String serviceName) {
        if (serviceName == null) return;
        registry.put(serviceName, Instant.now().toEpochMilli());
    }

    @KafkaListener(
        topics = "service-registry-topic",
        groupId = "frontend-registry-group",
        containerFactory = "traceKafkaListenerContainerFactory"
    )
    public void onAnnouncement(String jsonMessage) {
        try {
            Map<?, ?> msg      = objectMapper.readValue(jsonMessage, Map.class);
            String serviceName = (String) msg.get("serviceName");
            String status      = (String) msg.get("status");

            if ("DOWN".equals(status)) {
                // Only remove if NO other instance has checked in recently
                Long lastSeen = registry.get(serviceName);
                if (lastSeen != null &&
                    (Instant.now().toEpochMilli() - lastSeen) > 5_000) {
                    // last heartbeat was more than 5s ago — safe to mark down
                    registry.remove(serviceName);
                }
                System.out.println("📴 Service DOWN: " + serviceName);
            } else {
                register(serviceName);
                System.out.println("📡 Service UP: " + serviceName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns serviceName -> "UP" or "DOWN"
     * UP   = any instance heartbeated within 90s
     * DOWN = no heartbeat for 90s or explicit DOWN received
     */
    public Map<String, String> getSnapshot() {
        long now = Instant.now().toEpochMilli();
        Map<String, String> snapshot = new LinkedHashMap<>();

        // Remove fully stale entries first
        registry.entrySet().removeIf(e -> (now - e.getValue()) > STALE_THRESHOLD_MS);

        registry.forEach((name, lastSeen) ->
            snapshot.put(name, "UP")
        );

        return snapshot;
    }
}