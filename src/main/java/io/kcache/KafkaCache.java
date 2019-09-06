/*
 * Copyright 2014-2018 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kcache;

import io.kcache.exceptions.CacheException;
import io.kcache.exceptions.CacheInitializationException;
import io.kcache.exceptions.CacheTimeoutException;
import io.kcache.utils.InMemoryCache;
import io.kcache.utils.ShutdownableThread;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaCache<K, V> implements Cache<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaCache.class);

    private String topic;
    private int desiredReplicationFactor;
    private String groupId;
    private String clientId;
    private CacheUpdateHandler<K, V> cacheUpdateHandler;
    private Serde<K> keySerde;
    private Serde<V> valueSerde;
    private Cache<K, V> localCache;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private int initTimeout;
    private int timeout;
    private String bootstrapBrokers;
    private Producer<byte[], byte[]> producer;
    private Consumer<byte[], byte[]> consumer;
    private WorkerThread kafkaTopicReader;
    private KafkaCacheConfig config;

    public KafkaCache(String bootstrapServers,
                      Serde<K> keySerde,
                      Serde<V> valueSerde) {
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        setUp(new KafkaCacheConfig(props), keySerde, valueSerde, null, new InMemoryCache<>());
    }

    public KafkaCache(KafkaCacheConfig config,
                      Serde<K> keySerde,
                      Serde<V> valueSerde) {
        setUp(config, keySerde, valueSerde, null, new InMemoryCache<>());
    }

    public KafkaCache(KafkaCacheConfig config,
                      Serde<K> keySerde,
                      Serde<V> valueSerde,
                      CacheUpdateHandler<K, V> cacheUpdateHandler,
                      Cache<K, V> localCache) {
        setUp(config, keySerde, valueSerde, cacheUpdateHandler, localCache);
    }

    private void setUp(KafkaCacheConfig config,
                       Serde<K> keySerde,
                       Serde<V> valueSerde,
                       CacheUpdateHandler<K, V> cacheUpdateHandler,
                       Cache<K, V> localCache) {
        this.topic = config.getString(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG);
        this.desiredReplicationFactor = config.getInt(KafkaCacheConfig.KAFKACACHE_TOPIC_REPLICATION_FACTOR_CONFIG);
        this.groupId = config.getString(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG);
        this.clientId = config.getString(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG);
        if (this.clientId == null) {
        	this.clientId = "kafka-cache-reader-" + this.topic;
        }
        this.initTimeout = config.getInt(KafkaCacheConfig.KAFKACACHE_INIT_TIMEOUT_CONFIG);
        this.timeout = config.getInt(KafkaCacheConfig.KAFKACACHE_TIMEOUT_CONFIG);
        this.cacheUpdateHandler = cacheUpdateHandler != null ? cacheUpdateHandler : (key, value) -> {
        };
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.localCache = localCache;
        this.config = config;
        this.bootstrapBrokers = config.bootstrapBrokers();

        log.info("Initializing Kafka cache with broker endpoints: " + this.bootstrapBrokers);
    }

    @Override
    public void init() throws CacheInitializationException {
        if (initialized.get()) {
            throw new CacheInitializationException(
                "Illegal state while initializing cache. Cache was already initialized");
        }
        localCache.init();

        createOrVerifyTopic();
        this.producer = createProducer();
        this.consumer = createConsumer();

        // start the background thread that subscribes to the Kafka topic and applies updates.
        // the thread must be created after the topic has been created.
        this.kafkaTopicReader = new WorkerThread();
        this.kafkaTopicReader.readToEnd();
        this.kafkaTopicReader.start();

        boolean isInitialized = initialized.compareAndSet(false, true);
        if (!isInitialized) {
            throw new CacheInitializationException("Illegal state while initializing cache. Cache "
                + "was already initialized");
        }
    }

    private Consumer<byte[], byte[]> createConsumer() {
        Properties consumerProps = new Properties();
        addKafkaCacheConfigsToClientProperties(consumerProps);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        return new KafkaConsumer<>(consumerProps);
    }

    private Producer<byte[], byte[]> createProducer() {
        Properties producerProps = new Properties();
        addKafkaCacheConfigsToClientProperties(producerProps);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0); // Producer should not retry

        return new KafkaProducer<>(producerProps);
    }

    private void addKafkaCacheConfigsToClientProperties(Properties props) {
        props.putAll(config.originalsWithPrefix("kafkacache."));
    }

    private void createOrVerifyTopic() throws CacheInitializationException {
        Properties props = new Properties();
        addKafkaCacheConfigsToClientProperties(props);
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);

        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> allTopics = admin.listTopics().names().get(initTimeout, TimeUnit.MILLISECONDS);
            if (allTopics.contains(topic)) {
                verifyTopic(admin);
            } else {
                createTopic(admin);
            }
        } catch (TimeoutException e) {
            throw new CacheInitializationException(
                "Timed out trying to create or validate topic configuration",
                e
            );
        } catch (InterruptedException | ExecutionException e) {
            throw new CacheInitializationException(
                "Failed trying to create or validate topic configuration",
                e
            );
        }
    }

    private void createTopic(AdminClient admin) throws CacheInitializationException,
        InterruptedException, ExecutionException, TimeoutException {
        log.info("Creating topic {}", topic);

        int numLiveBrokers = admin.describeCluster().nodes().get(initTimeout, TimeUnit.MILLISECONDS).size();
        if (numLiveBrokers <= 0) {
            throw new CacheInitializationException("No live Kafka brokers");
        }

        int topicReplicationFactor = Math.min(numLiveBrokers, desiredReplicationFactor);
        if (topicReplicationFactor < desiredReplicationFactor) {
            log.warn("Creating the topic "
                + topic
                + " using a replication factor of "
                + topicReplicationFactor
                + ", which is less than the desired one of "
                + desiredReplicationFactor + ". If this is a production environment, it's "
                + "crucial to add more brokers and increase the replication factor of the topic.");
        }

        NewTopic topicRequest = new NewTopic(topic, 1, (short) topicReplicationFactor);
        topicRequest.configs(
            Collections.singletonMap(
                TopicConfig.CLEANUP_POLICY_CONFIG,
                TopicConfig.CLEANUP_POLICY_COMPACT
            )
        );
        try {
            admin.createTopics(Collections.singleton(topicRequest)).all()
                .get(initTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                // If topic already exists, ensure that it is configured correctly.
                verifyTopic(admin);
            } else {
                throw e;
            }
        }
    }

    private void verifyTopic(AdminClient admin) throws CacheInitializationException,
        InterruptedException, ExecutionException, TimeoutException {
        log.info("Validating topic {}", topic);

        Set<String> topics = Collections.singleton(topic);
        Map<String, TopicDescription> topicDescription;
        try {
            topicDescription = admin.describeTopics(topics).all().get(initTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                log.warn("Could not verify existing topic.");
                return;
            } else {
                throw e;
            }
        }

        TopicDescription description = topicDescription.get(topic);
        final int numPartitions = description.partitions().size();
        if (numPartitions != 1) {
            throw new CacheInitializationException("The topic " + topic + " should have only 1 "
                + "partition but has " + numPartitions);
        }

        if (description.partitions().get(0).replicas().size() < desiredReplicationFactor) {
            log.warn("The replication factor of the topic "
                + topic
                + " is less than the desired one of "
                + desiredReplicationFactor
                + ". If this is a production environment, it's crucial to add more brokers and "
                + "increase the replication factor of the topic.");
        }

        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

        Map<ConfigResource, Config> configs =
            admin.describeConfigs(Collections.singleton(topicResource)).all()
                .get(initTimeout, TimeUnit.MILLISECONDS);
        Config topicConfigs = configs.get(topicResource);
        String retentionPolicy = topicConfigs.get(TopicConfig.CLEANUP_POLICY_CONFIG).value();
        if (!TopicConfig.CLEANUP_POLICY_COMPACT.equals(retentionPolicy)) {
            log.error("The retention policy of the topic " + topic + " is incorrect. "
                + "You must configure the topic to 'compact' cleanup policy to avoid Kafka "
                + "deleting your data after a week. "
                + "Refer to Kafka documentation for more details on cleanup policies");

            throw new CacheInitializationException("The retention policy of the topic " + topic
                + " is incorrect. Expected cleanup.policy to be "
                + "'compact' but it is " + retentionPolicy);
        }
    }

    @Override
    public int size() {
        assertInitialized();
        return localCache.size();
    }

    @Override
    public boolean isEmpty() {
        assertInitialized();
        return localCache.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        assertInitialized();
        return localCache.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        assertInitialized();
        return localCache.containsValue(value);
    }

    @Override
    public V get(Object key) {
        assertInitialized();
        return localCache.get(key);
    }

    @Override
    public V put(K key, V value) {
        if (key == null) {
            throw new CacheException("Key should not be null");
        }

        assertInitialized();
        V oldValue = get(key);

        // write to the Kafka topic
        ProducerRecord<byte[], byte[]> producerRecord;
        try {
            producerRecord =
                new ProducerRecord<>(topic, 0, this.keySerde.serializer().serialize(topic, key),
                    value == null ? null : this.valueSerde.serializer().serialize(topic, value));
        } catch (Exception e) {
            throw new CacheException("Error serializing key while creating the Kafka produce record", e);
        }

        try {
            log.trace("Sending record to Kafka cache topic: " + producerRecord);
            Future<RecordMetadata> ack = producer.send(producerRecord);
            RecordMetadata recordMetadata = ack.get(timeout, TimeUnit.MILLISECONDS);

            log.trace("Waiting for the local cache to catch up to offset " + recordMetadata.offset());
            long lastWrittenOffset = recordMetadata.offset();
            kafkaTopicReader.waitUntilOffset(lastWrittenOffset, Duration.ofMillis(timeout));
        } catch (InterruptedException e) {
            throw new CacheException("Put operation interrupted while waiting for an ack from Kafka", e);
        } catch (ExecutionException e) {
            throw new CacheException("Put operation failed while waiting for an ack from Kafka", e);
        } catch (TimeoutException e) {
            throw new CacheTimeoutException(
                "Put operation timed out while waiting for an ack from Kafka", e);
        } catch (KafkaException ke) {
            throw new CacheException("Put operation to Kafka failed", ke);
        }

        return oldValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        assertInitialized();
        // TODO: write to the Kafka topic as a batch
        for (Map.Entry<? extends K, ? extends V> entry : entries.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        assertInitialized();
        // delete from the Kafka topic by writing a null value for the key
        return put((K) key, null);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
        assertInitialized();
        return localCache.keySet();
    }

    @Override
    public Collection<V> values() {
        assertInitialized();
        return localCache.values();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        assertInitialized();
        return localCache.entrySet();
    }

    @Override
    public Cache<K, V> subCache(K from, boolean fromInclusive, K to, boolean toInclusive) {
        assertInitialized();
        return localCache.subCache(from, fromInclusive, to, toInclusive);
    }

    @Override
    public KeyValueIterator<K, V> range(K from, boolean fromInclusive, K to, boolean toInclusive) {
        assertInitialized();
        return localCache.range(from, fromInclusive, to, toInclusive);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        assertInitialized();
        return localCache.all();
    }

    @Override
    public void close() throws IOException {
        if (kafkaTopicReader != null) {
            try {
                kafkaTopicReader.shutdown();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        if (producer != null) {
            producer.close();
            log.debug("Kafka cache producer shut down");
        }
        localCache.close();
        log.debug("Kafka cache shut down complete");
    }

    private void assertInitialized() throws CacheException {
        if (!initialized.get()) {
            throw new CacheException("Illegal state. Cache not initialized yet");
        }
    }

    /*
     * For testing.
     */
    WorkerThread getWorkerThread() {
        return this.kafkaTopicReader;
    }

    /**
     * Thread that reads data from the Kafka compacted topic and modifies
     * the local cache to be consistent.
     *
     * <p>On startup, this thread will always read from the beginning of the topic. We assume
     * the topic will always be small, hence the startup time to read the topic won't take
     * too long. Because the topic is always read from the beginning, the consumer never
     * commits offsets.
     */
    private class WorkerThread extends ShutdownableThread {

        private final TopicPartition topicPartition;
        private final ReentrantLock offsetUpdateLock;
        private final Condition offsetReachedThreshold;
        private long offsetInTopic = -1L;

        public WorkerThread() {
            super("kafka-cache-reader-thread-" + topic);
            offsetUpdateLock = new ReentrantLock();
            offsetReachedThreshold = offsetUpdateLock.newCondition();

            // Include a few retries since topic creation may take some time to propagate and
            // cache is often started immediately after creating the topic.
            int retries = 0;
            List<PartitionInfo> partitions = null;
            while (retries++ < 10) {
                partitions = consumer.partitionsFor(topic);
                if (partitions != null && partitions.size() >= 1) {
                    break;
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            }

            if (partitions == null || partitions.size() < 1) {
                throw new IllegalArgumentException("Unable to subscribe to the Kafka topic "
                    + topic
                    + " backing this data cache. Topic may not exist.");
            } else if (partitions.size() > 1) {
                throw new IllegalStateException("Unexpected number of partitions in the "
                    + topic
                    + " topic. Expected 1 and instead got " + partitions.size());
            }

            this.topicPartition = new TopicPartition(topic, 0);
            consumer.assign(Collections.singletonList(this.topicPartition));
            consumer.seekToBeginning(Collections.singletonList(this.topicPartition));

            log.info("Initialized last consumed offset to " + offsetInTopic);

            log.debug("KafkaTopicReader thread started.");
        }

        private void readToEnd() {
            Set<TopicPartition> assignment = consumer.assignment();
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);
            log.trace("Reading to end of offsets {}", endOffsets);

            while (!endOffsets.isEmpty()) {
                Iterator<Entry<TopicPartition, Long>> it = endOffsets.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<TopicPartition, Long> entry = it.next();
                    if (consumer.position(entry.getKey()) >= entry.getValue())
                        it.remove();
                    else {
                        poll();
                        break;
                    }
                }
            }
        }

        @Override
        protected void doWork() {
            poll();
        }

        private void poll() {
            try {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    K messageKey;
                    try {
                        messageKey = keySerde.deserializer().deserialize(topic, record.key());
                    } catch (Exception e) {
                        log.error("Failed to deserialize the key", e);
                        continue;
                    }

                    V message;
                    try {
                        message =
                            record.value() == null ? null
                                : valueSerde.deserializer().deserialize(topic, record.value());
                    } catch (Exception e) {
                        log.error("Failed to deserialize a value", e);
                        continue;
                    }
                    try {
                        if (cacheUpdateHandler.validateUpdate(messageKey, message)) {
                            log.trace("Applying update ("
                                + messageKey
                                + ","
                                + message
                                + ") to the local cache");
                            if (message == null) {
                                localCache.remove(messageKey);
                            } else {
                                localCache.put(messageKey, message);
                            }
                            cacheUpdateHandler.handleUpdate(messageKey, message);
                        } else {
                            if (!localCache.containsKey(messageKey)) {
                                try {
                                    ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
                                        topic,
                                        0,
                                        record.key(),
                                        null
                                    );
                                    producer.send(producerRecord);
                                } catch (KafkaException ke) {
                                    log.warn("Failed to tombstone invalid update", ke);
                                }
                            }
                        }
                        updateOffset(record.offset());
                    } catch (Exception se) {
                        log.error("Failed to add record from the Kafka topic"
                            + topic
                            + " to the local cache", se);
                    }
                }
            } catch (WakeupException we) {
                // do nothing because the thread is closing -- see shutdown()
            } catch (RecordTooLargeException rtle) {
                throw new IllegalStateException(
                    "Consumer threw RecordTooLargeException. Data has been written that "
                        + "exceeds the default maximum fetch size.", rtle);
            } catch (RuntimeException e) {
                log.error("KafkaTopicReader thread has died for an unknown reason.", e);
                throw e;
            }
        }

        private void updateOffset(long offset) {
            try {
                offsetUpdateLock.lock();
                offsetInTopic = offset;
                offsetReachedThreshold.signalAll();
            } finally {
                offsetUpdateLock.unlock();
            }
        }

        private void waitUntilOffset(long offset, Duration timeout) throws CacheException {
            if (offset < 0) {
                throw new CacheException("KafkaTopicReader thread can't wait for a negative offset.");
            }

            log.trace("Waiting to read offset {}. Currently at offset {}", offset, offsetInTopic);

            try {
                offsetUpdateLock.lock();
                long timeoutNs = timeout.toNanos();
                while ((offsetInTopic < offset) && (timeoutNs > 0)) {
                    try {
                        timeoutNs = offsetReachedThreshold.awaitNanos(timeoutNs);
                    } catch (InterruptedException e) {
                        log.debug("Interrupted while waiting for the background cache reader thread to reach"
                            + " the specified offset: " + offset, e);
                    }
                }
            } finally {
                offsetUpdateLock.unlock();
            }

            if (offsetInTopic < offset) {
                throw new CacheTimeoutException(
                    "KafkaCacheTopic thread failed to reach target offset within the timeout interval. "
                        + "targetOffset: " + offset + ", offsetReached: " + offsetInTopic
                        + ", timeout(ms): " + timeout.toMillis());
            }
        }

        @Override
        public void shutdown() throws InterruptedException {
            log.debug("Starting shutdown of KafkaTopicReader thread.");

            super.initiateShutdown();
            if (consumer != null) {
                consumer.wakeup();
            }
            super.awaitShutdown();
            if (consumer != null) {
                consumer.close();
            }
            log.info("KafkaTopicReader thread shutdown complete.");
        }
    }
}
