package com.aok.core.storage;

import com.aok.core.storage.message.Message;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaMessageStorage implements IStorage {

    private final ProducerPool producerPool;

    public KafkaMessageStorage(ProducerPool producerPool) {
        this.producerPool = producerPool;
    }

    @Override
    public void produce(Message message) {
        String key = CommonUtils.generateKey(message.getVhost(), message.getQueue());
        Producer<String, Message> producer = producerPool.getProducer(key);
        ProducerRecord<String, Message> record = new ProducerRecord<>(key,message);
        producer.send(record);
    }
}
