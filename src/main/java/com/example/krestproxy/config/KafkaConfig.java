package com.example.krestproxy.config;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;

@Configuration
public class KafkaConfig {

    @Bean
    public ObjectPool<Consumer<String, Object>> consumerPool(ConsumerFactory<String, Object> consumerFactory) {
        PooledKafkaConsumerFactory factory = new PooledKafkaConsumerFactory(consumerFactory);
        GenericObjectPoolConfig<Consumer<String, Object>> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(10); // Adjust based on needs
        config.setMaxIdle(5);
        config.setMinIdle(1);
        return new GenericObjectPool<>(factory, config);
    }
}
