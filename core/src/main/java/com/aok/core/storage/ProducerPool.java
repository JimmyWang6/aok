package com.aok.core.storage;

import com.aok.core.storage.message.Message;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProducerPool {
    private static class ProducerHolder {
        final Producer<String, Message> producer;
        volatile long lastAccess;
        ProducerHolder(Producer<String, Message> producer, long lastAccess) {
            this.producer = producer;
            this.lastAccess = lastAccess;
        }
    }

    private final ConcurrentHashMap<String, ProducerHolder> producerMap;
    private final Properties kafkaProps;
    private final long maxIdleTimeMillis;
    private final ScheduledExecutorService cleaner;

    public ProducerPool(Properties kafkaProps, long maxIdleTimeMillis) {
        this.kafkaProps = kafkaProps;
        producerMap = new ConcurrentHashMap<>();
        this.maxIdleTimeMillis = maxIdleTimeMillis;
        this.cleaner = Executors.newSingleThreadScheduledExecutor();
        this.cleaner.scheduleAtFixedRate(this::cleanIdleProducers, maxIdleTimeMillis, maxIdleTimeMillis, TimeUnit.MILLISECONDS);
    }

    public ProducerPool() {
        this.kafkaProps = new Properties();
        this.maxIdleTimeMillis = 60000; // default 60 seconds
        producerMap = new ConcurrentHashMap<>();
        this.cleaner = Executors.newSingleThreadScheduledExecutor();
        this.cleaner.scheduleAtFixedRate(this::cleanIdleProducers, maxIdleTimeMillis, maxIdleTimeMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Get a shared Producer instance by key. All threads for the same key get the same Producer.
     * Updates last access time.
     */
    public Producer<String, Message> getProducer(String key) {
        long now = System.currentTimeMillis();
        ProducerHolder holder = producerMap.compute(key, (k, old) -> {
            if (old == null) {
                Properties customProps = new Properties();
                customProps.putAll(kafkaProps);
                customProps.put("client.id", "producer-" + k);
                return new ProducerHolder(new org.apache.kafka.clients.producer.KafkaProducer<>(customProps), now);
            } else {
                old.lastAccess = now;
                return old;
            }
        });
        return holder.producer;
    }

    /**
     * Close all Producer instances and shutdown cleaner.
     */
    public void close() {
        cleaner.shutdownNow();
        producerMap.values().forEach(holder -> holder.producer.close());
        producerMap.clear();
    }

    /**
     * Remove and close idle producers.
     */
    private void cleanIdleProducers() {
        long now = System.currentTimeMillis();
        producerMap.forEach((key, holder) -> {
            if (now - holder.lastAccess > maxIdleTimeMillis) {
                if (producerMap.remove(key, holder)) {
                    holder.producer.close();
                }
            }
        });
    }
}
