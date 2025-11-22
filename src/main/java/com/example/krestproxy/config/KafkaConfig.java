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
    public ObjectPool<Consumer<Object, Object>> consumerPool(ConsumerFactory<Object, Object> consumerFactory) {
        var factory = new PooledKafkaConsumerFactory(consumerFactory);
        var config = new GenericObjectPoolConfig<Consumer<Object, Object>>();
        config.setMaxTotal(10); // Adjust based on needs
        config.setMaxIdle(5);
        config.setMinIdle(1);
        return new GenericObjectPool<>(factory, config);
    }
}
