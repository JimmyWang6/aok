package com.aok.meta.container;

import com.aok.meta.Meta;
import com.aok.meta.MetaKey;
import com.aok.meta.MetaType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
public class KafkaMetaContainer implements MetaContainer<Meta> {

    private static final String META_TOPIC = "meta";

    private KafkaProducer<MetaKey, Meta> producer;

    private final KafkaConsumer<MetaKey, Meta> consumer;

    private final KafkaAdminClient adminClient;

    private final ConcurrentHashMap<MetaKey, Meta> cache = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    public KafkaMetaContainer(String bootstrapServers) {
        try {
            adminClient = (KafkaAdminClient) createAdminClient(bootstrapServers);
            consumer = createConsumer(bootstrapServers);
            ensureMetaTopicExists();
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to subscribe to meta topic", e);
            throw new RuntimeException(e);
        }
        startConsumerThread();
    }

    /**
     * 启动单线程持续消费 META_TOPIC
     */
    private void startConsumerThread() {
        Thread consumerThread = new Thread(() -> {
            consumer.subscribe(List.of(META_TOPIC));
            while (true) {
                try {
                    var records = consumer.poll(java.time.Duration.ofMillis(500));
                    records.forEach(record -> {
                        log.debug("Consumed message: key={}, value={}, offset={}", record.key(), record.value(), record.offset());
                        lock.lock();
                        try {
                            Meta meta = cache.get(record.key());
                            if (record.value() == null) {
                                // a tombstone message.
                                cache.remove(record.key());
                                log.debug("Tombstone message processed, removed key: {} from cache", record.key());
                                return;
                            }
                            if (meta.getUuid().equals(record.value().getUuid())) {
                                log.info("Duplicate meta detected, skipping update for key: {}", record.key());
                                return;
                            }
                            cache.put(record.key(), record.value());
                        } finally {
                            lock.unlock();
                        }
                    });
                } catch (Exception e) {
                    log.error("Error during poll", e);
                }
            }
        }, "KafkaMetaConsumerThread");
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    @Override
    public Meta add(Meta meta) {
        lock.lock();
        try {
            MetaKey key = new MetaKey(getMetaType(meta), meta.getVhost(), meta.getName());
            cache.put(key, meta);
            persist(key, meta);
        } finally {
            lock.unlock();
        }
        return null;
    }

    @Override
    public Meta delete(Meta meta) {
        lock.lock();
        try {
            MetaKey key = new MetaKey(getMetaType(meta), meta.getVhost(), meta.getName());
            cache.remove(key);
            // tombstone message.
            ProducerRecord<MetaKey, Meta> record = new ProducerRecord<>(META_TOPIC, key, null);
            persist(record);
        } finally {
            lock.unlock();
        }
        return null;
    }

    @Override
    public void update(Meta meta) {
        lock.lock();
        try {
            MetaKey key = new MetaKey(getMetaType(meta), meta.getVhost(), meta.getName());
            cache.put(key, meta);
        } finally {
            lock.unlock();
        }

    }

    @Override
    public int size(Class<?> classType) {
        return cache.size();
    }

    @Override
    public List<Meta> list(Class<?> classType) {
        return cache.values().stream().filter(meta -> meta.getClass().equals(classType)).collect(Collectors.toList());
    }

    @Override
    public Meta get(Class<?> classType, String vhost, String name) {
        MetaType type = classType.getAnnotation(MetaType.class);
        if (type == null) {
            return null;
        }
        MetaKey key = new MetaKey(type.value(), vhost, name);
        return cache.get(key);
    }

    protected AdminClient createAdminClient(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        return AdminClient.create(props);
    }

    public void persist(MetaKey metaKey, Meta meta) {
        ProducerRecord<MetaKey, Meta> producerRecord = new ProducerRecord<>(META_TOPIC, metaKey, meta);
        try {
            producer.send(producerRecord).get();
        } catch (Exception e) {
            log.error("Failed to persist meta. key={}, meta={}, error={}", metaKey, meta, e.getMessage(), e);
        }
    }

    public void persist(ProducerRecord<MetaKey, Meta> producerRecord) {
        try {
            producer.send(producerRecord).get();
        } catch (Exception e) {
            log.error("Failed to persist meta. key={}, meta={}, error={}", producerRecord.key(), producerRecord.value(), e.getMessage(), e);
        }
    }

    /**
     * 创建 KafkaConsumer 实例
     */
    protected KafkaConsumer<MetaKey, Meta> createConsumer(String bootstrapServers) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrapServers);
        consumerProps.put("key.deserializer", "com.aok.meta.serialization.MetaKeyDeserializer");
        consumerProps.put("value.deserializer", "com.aok.meta.serialization.MetaDeserializer");
        consumerProps.put("group.id", "meta-group");
        consumerProps.put("auto.offset.reset", "earliest");
        return new KafkaConsumer<>(consumerProps);
    }

    /**
     * 从 META_TOPIC 最早 offset 开始消费
     */
    protected void consumeFromEarliest() {
        if (consumer == null) {
            log.warn("Consumer is not initialized.");
            return;
        }
        consumer.subscribe(List.of(META_TOPIC));
        consumer.poll(java.time.Duration.ofMillis(100));
        consumer.assignment().forEach(partition -> consumer.seekToBeginning(List.of(partition)));
        log.info("Consumer seeked to earliest for topic '{}'.", META_TOPIC);
    }

    /**
     * Ensure the meta topic exists in Kafka. If not, create it.
     */
    protected void ensureMetaTopicExists() throws ExecutionException, InterruptedException {
        ListTopicsResult listTopics = adminClient.listTopics(new ListTopicsOptions());
        Set<String> set = listTopics.listings().get().stream().map(TopicListing::name).collect(Collectors.toSet());
        if (!set.contains(META_TOPIC)) {
            NewTopic newTopic = new NewTopic(META_TOPIC, 3, (short) 2);
            CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
            createTopicsResult.all().get();
            log.info("Topic '{}' created.", META_TOPIC);
        } else {
            log.info("Topic '{}' already exists.", META_TOPIC);
        }
        consumeFromEarliest();
    }
}
