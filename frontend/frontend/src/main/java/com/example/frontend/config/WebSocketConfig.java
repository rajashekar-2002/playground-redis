package com.example.frontend.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker // Enables WebSocket with STOMP messaging
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        // Simple in-memory broker, /topic is where messages are broadcasted
        config.enableSimpleBroker("/topic");
        // Prefix for messages sent from clients to server methods
        config.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // Client connects to this URL to establish WebSocket
        registry.addEndpoint("/ws").withSockJS(); // SockJS fallback for browsers
    }
}

// Explanation:

// /ws → Frontend connects here (SockJS allows fallback if WebSocket not
// supported)

// /app → Client sends messages here, routed to @MessageMapping methods in
// controllers

// /topic → Server broadcasts messages here, subscribed by clients

// This separates sending (app) and receiving (topic), making it easier to
// manage real-time updates