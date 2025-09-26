package com.aok.meta.container;

import com.aok.meta.*;
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

    private final AdminClient adminClient;

    private final ConcurrentHashMap<MetaKey, Meta> cache = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    public KafkaMetaContainer(String bootstrapServers) {
        try {
            adminClient = createAdminClient(bootstrapServers);
            producer = createProducer(bootstrapServers);
            consumer = createConsumer(bootstrapServers);
            ensureMetaTopicExists();
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to subscribe to meta topic", e);
            throw new RuntimeException(e);
        }
        startConsumerThread();
    }

    protected KafkaProducer<MetaKey, Meta> createProducer(String bootstrapServers) {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("key.serializer", "com.aok.meta.serialization.MetaKeySerializer");
        producerProps.put("value.serializer", "com.aok.meta.serialization.MetaSerializer");
        producerProps.put("acks", "all");
        return new KafkaProducer<>(producerProps);
    }

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
                            if (meta != null && meta.getUuid().equals(record.value().getUuid())) {
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
//            cache.put(key, meta);
            persist(key, meta);
            return meta;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Meta delete(Meta meta) {
        lock.lock();
        try {
            MetaKey key = new MetaKey(getMetaType(meta), meta.getVhost(), meta.getName());
//            cache.remove(key);
            // tombstone message.
            ProducerRecord<MetaKey, Meta> record = new ProducerRecord<>(META_TOPIC, key, null);
            persist(record);
            return meta;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void update(Meta meta) {
        lock.lock();
        try {
            MetaKey key = new MetaKey(getMetaType(meta), meta.getVhost(), meta.getName());
//            cache.put(key, meta);
            persist(key, meta);
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

    protected KafkaConsumer<MetaKey, Meta> createConsumer(String bootstrapServers) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrapServers);
        consumerProps.put("key.deserializer", "com.aok.meta.serialization.MetaKeyDeserializer");
        consumerProps.put("value.deserializer", "com.aok.meta.serialization.MetaDeserializer");
        consumerProps.put("group.id", "meta-group");
        consumerProps.put("auto.offset.reset", "earliest");
        return new KafkaConsumer<>(consumerProps);
    }

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
            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put("cleanup.policy", "compact");
            topicConfig.put("segment.ms", "60000");
            topicConfig.put("min.cleanable.dirty.ratio", "0.1");
            NewTopic newTopic = new NewTopic(META_TOPIC, 1, (short) 1).configs(topicConfig);
            CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
            createTopicsResult.all().get();
            log.info("Topic '{}' created as compact.", META_TOPIC);
        } else {
            log.info("Topic '{}' already exists and is compact.", META_TOPIC);
        }
        consumeFromEarliest();
    }

    public void close() {
        try {
            if (producer != null) producer.close();
        } catch (Exception e) {
            log.warn("Error closing producer", e);
        }
        try {
            if (consumer != null) consumer.close();
        } catch (Exception e) {
            log.warn("Error closing consumer", e);
        }
        try {
            if (adminClient != null) adminClient.close();
        } catch (Exception e) {
            log.warn("Error closing adminClient", e);
        }
    }

    public static void main(String[] args) {
        // 请根据实际 Kafka 地址修改
        String bootstrapServers = "localhost:9092";
        KafkaMetaContainer container = new KafkaMetaContainer(bootstrapServers);
        try {
            Exchange exchange = new Exchange("testVhost", "testName", ExchangeType.Direct, true, true, true, null);
            exchange.setUuid(UUID.randomUUID());

            // 添加 Exchange
            System.out.println("Add: " + container.add(exchange));
            Thread.sleep(2000);
            List<Exchange> exchanges = (List<Exchange>) (List<?>) container.list(exchange.getClass());
            System.out.println("List: " + exchanges);

            // 更新 Exchange
            exchange.setName("testNameUpdated");
            container.update(exchange);
            Thread.sleep(2000);
            exchanges = (List<Exchange>) (List<?>) container.list(exchange.getClass());
            System.out.println("List: " + exchanges);

            // 获取 Exchange
            Exchange fetched = (Exchange) container.get(exchange.getClass(), exchange.getVhost(), exchange.getName());
            System.out.println("Get: " + fetched);
            Thread.sleep(2000);
            exchanges = (List<Exchange>) (List<?>) container.list(exchange.getClass());
            System.out.println("List: " + exchanges);


            // 删除 Exchange
            System.out.println("Delete: " + container.delete(exchange));
            Thread.sleep(2000);
            exchanges = (List<Exchange>) (List<?>) container.list(exchange.getClass());
            System.out.println("List: " + exchanges);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            container.close();
        }
    }
}
