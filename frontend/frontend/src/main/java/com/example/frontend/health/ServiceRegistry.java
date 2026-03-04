package com.example.frontend.health;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

@Component
public class ServiceRegistry {

    // serviceName -> Set of unique serviceIds seen
    private final Map<String, Set<String>> registry = new ConcurrentHashMap<>();

    /**
     * Called every time a TraceMessage arrives.
     * Registers the serviceId under the serviceName.
     */
    public void register(String serviceName, String serviceId) {
        if (serviceName == null || serviceId == null) return;
        registry
            .computeIfAbsent(serviceName, k -> ConcurrentHashMap.newKeySet())
            .add(serviceId);
    }

    /**
     * Returns a snapshot: serviceName -> sorted list of serviceIds
     */
    public Map<String, List<String>> getSnapshot() {
        Map<String, List<String>> snapshot = new LinkedHashMap<>();
        registry.forEach((name, ids) -> {
            List<String> sorted = new ArrayList<>(ids);
            Collections.sort(sorted);
            snapshot.put(name, sorted);
        });
        return snapshot;
    }
}