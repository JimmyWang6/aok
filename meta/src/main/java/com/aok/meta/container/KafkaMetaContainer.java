/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aok.meta.container;

import com.aok.meta.Dummy;
import com.aok.meta.Meta;
import com.aok.meta.MetaKey;
import com.aok.meta.MetaType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
public class KafkaMetaContainer implements MetaContainer<Meta> {

    public static final String DUMMY = "dummy";
    public static final int TIMEOUT = 30;
    private volatile boolean running = true;

    private static final String META_TOPIC = "meta-topic";

    private KafkaProducer<MetaKey, Meta> producer;

    private KafkaConsumer<MetaKey, Meta> consumer;

    private volatile long latestOffset = -1;

    private final CountDownLatch latch = new CountDownLatch(1);

    private final String bootstrapServers;

    private AdminClient adminClient;

    private final ConcurrentHashMap<MetaKey, Meta> cache = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    public KafkaMetaContainer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Produce a dummy message to get the latest offset.
     *
     */
    protected long getLatestOffset() {
        MetaKey metaKey = new MetaKey(DUMMY, DUMMY, DUMMY);
        Meta dummy = new Dummy();
        ProducerRecord<MetaKey, Meta> producerRecord = new ProducerRecord<>(META_TOPIC, metaKey, dummy);
        try {
            RecordMetadata metadata = producer.send(producerRecord).get(10, TimeUnit.SECONDS);
            return metadata.offset();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    protected KafkaProducer<MetaKey, Meta> createProducer(String bootstrapServers) {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("key.serializer", "com.aok.meta.serialization.MetaKeySerializer");
        producerProps.put("value.serializer", "com.aok.meta.serialization.MetaSerializer");
        producerProps.put("acks", "1");
        producerProps.put("max.block.ms", "10000");
        return new KafkaProducer<>(producerProps);
    }

    private void startConsumerThread(String bootstrapServers) {
        Thread consumerThread = new Thread(() -> {
            consumer = createConsumer(bootstrapServers);
            consumeFromEarliest();
            while (running) {
                try {
                    var records = consumer.poll(java.time.Duration.ofMillis(500));
                    records.forEach(record -> {
                        log.debug("Consumed message: key={}, value={}, offset={}", record.key(), record.value(), record.offset());
                        lock.lock();
                        try {
                            Meta meta = cache.get(record.key());
                            if (record.offset() >= latestOffset) {
                                latch.countDown();
                            }
                            if (record.value() == null) {
                                // a tombstone message.
                                cache.remove(record.key());
                                log.debug("Tombstone message processed, removed key: {} from cache", record.key());
                                return;
                            }
                            if (meta != null && record.offset() <= meta.getOffset()) {
                                log.debug("Duplicate meta detected, skipping update for key: {}", record.key());
                                return;
                            }
                            Meta cur = record.value();
                            cur.setOffset(record.offset());
                            MetaKey key = new MetaKey(cur.getMetaType(), cur.getVhost(), cur.getName());
                            cache.put(key, cur);
                        } finally {
                            lock.unlock();
                        }
                    });
                    consumer.commitAsync();
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
            MetaKey key = new MetaKey(meta.getMetaType(), meta.getVhost(), meta.getName());
            RecordMetadata record = persist(key, meta);
            meta.setOffset(record.offset());
            cache.put(key, meta);
            return meta;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Meta delete(Meta meta) {
        lock.lock();
        try {
            MetaKey key = new MetaKey(meta.getMetaType(), meta.getVhost(), meta.getName());
            // tombstone message.
            ProducerRecord<MetaKey, Meta> record = new ProducerRecord<>(META_TOPIC, key, null);
            persist(record);
            cache.remove(key);
            return meta;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void update(Meta meta) {
        lock.lock();
        try {
            MetaKey key = new MetaKey(meta.getMetaType(), meta.getVhost(), meta.getName());
            RecordMetadata record = persist(key, meta);
            meta.setOffset(record.offset());
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

    public RecordMetadata persist(MetaKey metaKey, Meta meta) {
        ProducerRecord<MetaKey, Meta> producerRecord = new ProducerRecord<>(META_TOPIC, metaKey, meta);
        try {
            RecordMetadata recordMetadata = producer.send(producerRecord).get();
            log.debug("Persisted meta. key={}, meta={}, offset={}", metaKey, meta, recordMetadata.offset());
            return recordMetadata;
        } catch (Exception e) {
            log.error("Failed to persist meta. key={}, meta={}, error={}", metaKey, meta, e.getMessage(), e);
            return null;
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
    }

    public void start() {
        try {
            adminClient = createAdminClient(bootstrapServers);
            producer = createProducer(bootstrapServers);
            this.latestOffset = getLatestOffset();
            ensureMetaTopicExists();
            startConsumerThread(bootstrapServers);
            // wait until the consumer has caught up to the latest offset.
            latch.await(TIMEOUT, TimeUnit.SECONDS);
            log.info("Meta container started");
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to subscribe to meta topic", e);
            throw new RuntimeException(e);
        }
    }

    public void close() {
        running = false;
        try {
            if (producer != null) producer.close();
        } catch (Exception e) {
            log.warn("Error closing producer", e);
        }
        try {
            if (adminClient != null) adminClient.close();
        } catch (Exception e) {
            log.warn("Error closing adminClient", e);
        }
    }

    public String print() {
        StringBuilder sb = new StringBuilder();
        cache.values().forEach(meta -> sb.append(meta.toString()).append(System.lineSeparator()));
        return sb.toString();
    }
}
