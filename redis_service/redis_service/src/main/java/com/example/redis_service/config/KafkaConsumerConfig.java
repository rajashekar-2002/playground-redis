package com.example.redis_service.config;

import com.example.redis_service.dto.RedisEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, RedisEvent> consumerFactory() {

        JsonDeserializer<RedisEvent> deserializer = new JsonDeserializer<>(RedisEvent.class);

        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(false);
        deserializer.setRemoveTypeHeaders(true); // IMPORTANT
        deserializer.setUseTypeHeaders(false); // VERY IMPORTANT

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "redis-service-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

               // Prevent ghost consumers from accumulating
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000); // detect dead consumer in 6s (was 45s)
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 2000); // heartbeat every 2s
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 10000); // kick if no poll in 10s
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 8000);


        return new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(),
                deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RedisEvent> kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, RedisEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}