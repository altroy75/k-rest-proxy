package com.example.krestproxy.config;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.core.ConsumerFactory;

public class PooledKafkaConsumerFactory extends BasePooledObjectFactory<Consumer<String, Object>> {

    private final ConsumerFactory<String, Object> consumerFactory;

    public PooledKafkaConsumerFactory(ConsumerFactory<String, Object> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    @Override
    public Consumer<String, Object> create() throws Exception {
        return consumerFactory.createConsumer();
    }

    @Override
    public PooledObject<Consumer<String, Object>> wrap(Consumer<String, Object> consumer) {
        return new DefaultPooledObject<>(consumer);
    }

    @Override
    public void destroyObject(PooledObject<Consumer<String, Object>> p) throws Exception {
        p.getObject().close();
    }
}
