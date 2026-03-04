// package com.playground.trace.service;

// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.playground.trace.dto.TraceMessage;
// import org.springframework.kafka.annotation.KafkaListener;
// import org.springframework.kafka.core.KafkaTemplate;
// import org.springframework.stereotype.Service;

// @Service
// public class TraceServiceHandler {

//     private final KafkaTemplate<String, String> kafkaTemplate;
//     private final ObjectMapper objectMapper = new ObjectMapper();

//     public TraceServiceHandler(KafkaTemplate<String, String> kafkaTemplate) {
//         this.kafkaTemplate = kafkaTemplate;
//     }

//     @KafkaListener(topics = "trace-topic", groupId = "trace-service-group")
//     public void handleTrace(String jsonMessage) {
//         try {
//             TraceMessage message = objectMapper.readValue(jsonMessage, TraceMessage.class);
//             System.out.println("Trace received: " + message);

//             // Add trace-service info
//             message.setServiceName("trace-service");

//             // Send back as JSON string
//             String responseJson = objectMapper.writeValueAsString(message);
//             kafkaTemplate.send("trace-response-topic", responseJson);

//             System.out.println("Trace response sent: " + responseJson);
//         } catch (Exception e) {
//             System.err.println("Failed to process trace message: " + e.getMessage());
//             e.printStackTrace();
//         }
//     }
// }