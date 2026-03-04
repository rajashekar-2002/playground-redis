package com.example.frontend.TraceListener.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class TraceKafkaConfig {

    private final String bootstrapServers = "localhost:9092";

    // ---------------- Producer ----------------
    @Bean
    public ProducerFactory<String, String> traceProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> traceKafkaTemplate() {
        return new KafkaTemplate<>(traceProducerFactory());
    }

    // ---------------- Consumer ----------------
    @Bean
    public ConsumerFactory<String, String> traceConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "frontend-trace-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // don't replay old messages on restart

        // Prevent ghost consumers from accumulating
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000); // detect dead consumer in 6s (was 45s)
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 2000); // heartbeat every 2s
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 10000); // kick if no poll in 10s
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 8000);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> traceKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(traceConsumerFactory());
        // Clean shutdown - commits offset and closes connection properly before app
        // stops
        factory.getContainerProperties().setShutdownTimeout(5000L);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}