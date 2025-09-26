package com.aok.core.storage;

import com.aok.core.storage.message.Message;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import java.util.Properties;

/**
 * KeyProducerFactory is used to create and manage KafkaProducer objects by key.
 * Each key can have its own Producer instance.
 */
public class KeyProducerFactory extends BaseKeyedPooledObjectFactory<String, Producer<String, Message>> {

    private final Properties props;

    public KeyProducerFactory(Properties props) {
        this.props = props;
    }

    @Override
    public Producer<String, Message> create(String key) {
        Properties customProps = new Properties();
        customProps.putAll(props);
        customProps.put("client.id", "producer-" + key);
        return new KafkaProducer<>(customProps);
    }

    @Override
    public PooledObject<Producer<String, Message>> wrap(Producer<String, Message> producer) {
        return new DefaultPooledObject<>(producer);
    }

    @Override
    public void destroyObject(String key, PooledObject<Producer<String, Message>> p) {
        p.getObject().close();
    }
}
