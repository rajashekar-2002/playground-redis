package com.example.frontend.config;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.example.frontend.dto.RedisResponseEvent;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, RedisResponseEvent> consumerFactory() {

        JsonDeserializer<RedisResponseEvent> deserializer = new JsonDeserializer<>(RedisResponseEvent.class);

        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(false);
        deserializer.setUseTypeHeaders(false);
        deserializer.setRemoveTypeHeaders(true);

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "frontend-group" + UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Prevent ghost consumers from accumulating
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000); // detect dead consumer in 6s (was 45s)
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 2000); // heartbeat every 2s
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 10000); // kick if no poll in 10s
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 8000);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RedisResponseEvent> kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, RedisResponseEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        return factory;
    }
}