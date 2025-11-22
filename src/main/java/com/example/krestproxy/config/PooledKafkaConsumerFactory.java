package com.example.krestproxy.config;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.core.ConsumerFactory;

public class PooledKafkaConsumerFactory extends BasePooledObjectFactory<Consumer<Object, Object>> {

    private final ConsumerFactory<Object, Object> consumerFactory;

    public PooledKafkaConsumerFactory(ConsumerFactory<Object, Object> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    @Override
    public Consumer<Object, Object> create() throws Exception {
        return consumerFactory.createConsumer();
    }

    @Override
    public PooledObject<Consumer<Object, Object>> wrap(Consumer<Object, Object> consumer) {
        return new DefaultPooledObject<>(consumer);
    }

    @Override
    public void destroyObject(PooledObject<Consumer<Object, Object>> p) throws Exception {
        p.getObject().close();
    }
}
