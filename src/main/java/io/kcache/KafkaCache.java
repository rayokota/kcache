/**
 * Copyright 2014 Confluent Inc.
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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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

    private final String topic;
    private final int desiredReplicationFactor;
    private final String groupId;
    private final CacheUpdateHandler<K, V> cacheUpdateHandler;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Cache<K, V> localCache;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final int initTimeout;
    private final int timeout;
    private final String bootstrapBrokers;
    private KafkaProducer<byte[], byte[]> producer;
    private KafkaConsumer<byte[], byte[]> consumer;
    private WorkerThread<K, V> kafkaTopicReader;
    // Noop key is only used to help reliably determine last offset; reader thread ignores
    // messages with this key
    private final K noopKey;
    private volatile long lastWrittenOffset = -1L;
    private final CacheConfig config;

    public KafkaCache(CacheConfig config,
                      CacheUpdateHandler<K, V> cacheUpdateHandler,
                      Serde<K> keySerde,
                      Serde<V> valueSerde,
                      Cache<K, V> localCache,
                      K noopKey) throws CacheInitializationException {
        this.topic = config.getString(CacheConfig.KAFKASTORE_TOPIC_CONFIG);
        this.desiredReplicationFactor =
            config.getInt(CacheConfig.KAFKASTORE_TOPIC_REPLICATION_FACTOR_CONFIG);
        this.groupId = config.getString(CacheConfig.KAFKASTORE_GROUP_ID_CONFIG);
        initTimeout = config.getInt(CacheConfig.KAFKASTORE_INIT_TIMEOUT_CONFIG);
        timeout = config.getInt(CacheConfig.KAFKASTORE_TIMEOUT_CONFIG);
        this.cacheUpdateHandler = cacheUpdateHandler;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.localCache = localCache;
        this.noopKey = noopKey;
        this.config = config;
        this.bootstrapBrokers = config.bootstrapBrokers();

        log.info("Initializing KafkaStore with broker endpoints: " + this.bootstrapBrokers);
    }

    @Override
    public void init() throws CacheInitializationException {
        if (initialized.get()) {
            throw new CacheInitializationException(
                "Illegal state while initializing store. Store was already initialized");
        }

        createOrVerifySchemaTopic();

        // set the producer properties and initialize a Kafka producer client
        Properties producerProps = new Properties();
        addSchemaRegistryConfigsToClientProperties(config, producerProps);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArraySerializer.class);
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0); // Producer should not retry

        producer = new KafkaProducer<byte[], byte[]>(producerProps);

        Properties consumerProps = new Properties();
        addSchemaRegistryConfigsToClientProperties(config, consumerProps);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaStore-reader-" + this.topic);

        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

        this.consumer = new KafkaConsumer<>(consumerProps);

        // start the background thread that subscribes to the Kafka topic and applies updates.
        // the thread must be created after the schema topic has been created.
        this.kafkaTopicReader =
            new WorkerThread<>(this.bootstrapBrokers, topic, groupId,
                this.cacheUpdateHandler, keySerde, valueSerde, this.localCache,
                this.noopKey, this.config);
        this.kafkaTopicReader.start();

        try {
            waitUntilKafkaReaderReachesLastOffset(initTimeout);
        } catch (CacheException e) {
            throw new CacheInitializationException(e);
        }

        boolean isInitialized = initialized.compareAndSet(false, true);
        if (!isInitialized) {
            throw new CacheInitializationException("Illegal state while initializing store. Store "
                + "was already initialized");
        }
    }

    public static void addSchemaRegistryConfigsToClientProperties(CacheConfig config,
                                                                  Properties props) {
        props.putAll(config.originalsWithPrefix("kafkastore."));
    }


    private void createOrVerifySchemaTopic() throws CacheInitializationException {
        Properties props = new Properties();
        addSchemaRegistryConfigsToClientProperties(this.config, props);
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);

        try (AdminClient admin = AdminClient.create(props)) {
            //
            Set<String> allTopics = admin.listTopics().names().get(initTimeout, TimeUnit.MILLISECONDS);
            if (allTopics.contains(topic)) {
                verifySchemaTopic(admin);
            } else {
                createSchemaTopic(admin);
            }
        } catch (TimeoutException e) {
            throw new CacheInitializationException(
                "Timed out trying to create or validate schema topic configuration",
                e
            );
        } catch (InterruptedException | ExecutionException e) {
            throw new CacheInitializationException(
                "Failed trying to create or validate schema topic configuration",
                e
            );
        }
    }

    private void createSchemaTopic(AdminClient admin) throws CacheInitializationException,
        InterruptedException,
        ExecutionException,
        TimeoutException {
        log.info("Creating schemas topic {}", topic);

        int numLiveBrokers = admin.describeCluster().nodes()
            .get(initTimeout, TimeUnit.MILLISECONDS).size();
        if (numLiveBrokers <= 0) {
            throw new CacheInitializationException("No live Kafka brokers");
        }

        int schemaTopicReplicationFactor = Math.min(numLiveBrokers, desiredReplicationFactor);
        if (schemaTopicReplicationFactor < desiredReplicationFactor) {
            log.warn("Creating the schema topic "
                + topic
                + " using a replication factor of "
                + schemaTopicReplicationFactor
                + ", which is less than the desired one of "
                + desiredReplicationFactor + ". If this is a production environment, it's "
                + "crucial to add more brokers and increase the replication factor of the topic.");
        }

        NewTopic schemaTopicRequest = new NewTopic(topic, 1, (short) schemaTopicReplicationFactor);
        schemaTopicRequest.configs(
            Collections.singletonMap(
                TopicConfig.CLEANUP_POLICY_CONFIG,
                TopicConfig.CLEANUP_POLICY_COMPACT
            )
        );
        try {
            admin.createTopics(Collections.singleton(schemaTopicRequest)).all()
                .get(initTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                // If topic already exists, ensure that it is configured correctly.
                verifySchemaTopic(admin);
            } else {
                throw e;
            }
        }
    }

    private void verifySchemaTopic(AdminClient admin) throws CacheInitializationException,
        InterruptedException,
        ExecutionException,
        TimeoutException {
        log.info("Validating schemas topic {}", topic);

        Set<String> topics = Collections.singleton(topic);
        Map<String, TopicDescription> topicDescription = admin.describeTopics(topics)
            .all().get(initTimeout, TimeUnit.MILLISECONDS);

        TopicDescription description = topicDescription.get(topic);
        final int numPartitions = description.partitions().size();
        if (numPartitions != 1) {
            throw new CacheInitializationException("The schema topic " + topic + " should have only 1 "
                + "partition but has " + numPartitions);
        }

        if (description.partitions().get(0).replicas().size() < desiredReplicationFactor) {
            log.warn("The replication factor of the schema topic "
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
        if (retentionPolicy == null || !TopicConfig.CLEANUP_POLICY_COMPACT.equals(retentionPolicy)) {
            log.error("The retention policy of the schema topic " + topic + " is incorrect. "
                + "You must configure the topic to 'compact' cleanup policy to avoid Kafka "
                + "deleting your schemas after a week. "
                + "Refer to Kafka documentation for more details on cleanup policies");

            throw new CacheInitializationException("The retention policy of the schema topic " + topic
                + " is incorrect. Expected cleanup.policy to be "
                + "'compact' but it is " + retentionPolicy);

        }
    }


    /**
     * Wait until the KafkaStore catches up to the last message in the Kafka topic.
     */
    public void waitUntilKafkaReaderReachesLastOffset(int timeoutMs) throws CacheException {
        long offsetOfLastMessage = getLatestOffset(timeoutMs);
        log.info("Wait to catch up until the offset of the last message at " + offsetOfLastMessage);
        kafkaTopicReader.waitUntilOffset(offsetOfLastMessage, timeoutMs, TimeUnit.MILLISECONDS);
        log.debug("Reached offset at " + offsetOfLastMessage);
    }

    public void markLastWrittenOffsetInvalid() {
        lastWrittenOffset = -1L;
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
        assertInitialized();
        if (key == null) {
            throw new CacheException("Key should not be null");
        }

        V oldValue = get(key);

        // write to the Kafka topic
        ProducerRecord<byte[], byte[]> producerRecord = null;
        try {
            producerRecord =
                new ProducerRecord<byte[], byte[]>(topic, 0, this.keySerde.serializer().serialize(topic, key),
                    value == null ? null : this.valueSerde.serializer().serialize(topic, value));
        } catch (Exception e) {
            throw new CacheException("Error serializing schema while creating the Kafka produce "
                + "record", e);
        }

        boolean knownSuccessfulWrite = false;
        try {
            log.trace("Sending record to KafkaStore topic: " + producerRecord);
            Future<RecordMetadata> ack = producer.send(producerRecord);
            RecordMetadata recordMetadata = ack.get(timeout, TimeUnit.MILLISECONDS);

            log.trace("Waiting for the local store to catch up to offset " + recordMetadata.offset());
            this.lastWrittenOffset = recordMetadata.offset();
            kafkaTopicReader.waitUntilOffset(this.lastWrittenOffset, timeout, TimeUnit.MILLISECONDS);
            knownSuccessfulWrite = true;
        } catch (InterruptedException e) {
            throw new CacheException("Put operation interrupted while waiting for an ack from Kafka", e);
        } catch (ExecutionException e) {
            throw new CacheException("Put operation failed while waiting for an ack from Kafka", e);
        } catch (TimeoutException e) {
            throw new CacheTimeoutException(
                "Put operation timed out while waiting for an ack from Kafka", e);
        } catch (KafkaException ke) {
            throw new CacheException("Put operation to Kafka failed", ke);
        } finally {
            if (!knownSuccessfulWrite) {
                this.lastWrittenOffset = -1L;
            }
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
        // deleteSchemaVersion from the Kafka topic by writing a null value for the key
        return put((K) key, null);
    }

    @Override
    public void clear() {
        log.debug("Shutting down Kafka store.");

        consumer.wakeup();

        try {
            kafkaTopicReader.join();
        } catch (InterruptedException e) {
            throw new CacheException("Failed to stop KafkaBasedLog. Exiting without cleanly shutting " +
                "down it's producer and consumer.", e);
        }

        try {
            producer.close();
        } catch (KafkaException e) {
            log.error("Failed to stop Kafka cache producer", e);
        }

        try {
            consumer.close();
        } catch (KafkaException e) {
            log.error("Failed to stop Kafka cache consumer", e);
        }

        localCache.clear();
        log.debug("Kafka store shut down complete");
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
    public void close() {
        clear();
    }

    /**
     * For testing.
     */
    WorkerThread<K, V> getWorkerThread() {
        return this.kafkaTopicReader;
    }

    private void assertInitialized() throws CacheException {
        if (!initialized.get()) {
            throw new CacheException("Illegal state. Store not initialized yet");
        }
    }

    /**
     * Return the latest offset of the store topic.
     *
     * <p>The most reliable way to do so in face of potential Kafka broker failure is to produce
     * successfully to the Kafka topic and get the offset of the returned metadata.
     *
     * <p>If the most recent write to Kafka was successful (signaled by lastWrittenOffset >= 0),
     * immediately return that offset. Otherwise write a "Noop key" to Kafka in order to find the
     * latest offset.
     */
    private long getLatestOffset(int timeoutMs) throws CacheException {
        ProducerRecord<byte[], byte[]> producerRecord = null;

        if (this.lastWrittenOffset >= 0) {
            return this.lastWrittenOffset;
        }

        try {
            producerRecord =
                new ProducerRecord<byte[], byte[]>(topic, 0, this.keySerde.serializer().serialize(topic, noopKey), null);
        } catch (Exception e) {
            throw new CacheException("Failed to serialize noop key.", e);
        }

        try {
            log.trace("Sending Noop record to KafkaStore to find last offset.");
            Future<RecordMetadata> ack = producer.send(producerRecord);
            RecordMetadata metadata = ack.get(timeoutMs, TimeUnit.MILLISECONDS);
            this.lastWrittenOffset = metadata.offset();
            log.trace("Noop record's offset is " + this.lastWrittenOffset);
            return this.lastWrittenOffset;
        } catch (Exception e) {
            throw new CacheException("Failed to write Noop record to kafka store.", e);
        }
    }

    /**
     * Thread that reads schema registry state from the Kafka compacted topic and modifies
     * the local store to be consistent.
     *
     * <p>On startup, this thread will always read from the beginning of the topic. We assume
     * the topic will always be small, hence the startup time to read the topic won't take
     * too long. Because the topic is always read from the beginning, the consumer never
     * commits offsets.
     */
    public static class WorkerThread<K, V> extends Thread {

        private final String topic;
        private final TopicPartition topicPartition;
        private final String groupId;
        private final CacheUpdateHandler<K, V> cacheUpdateHandler;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;
        private final Cache<K, V> localCache;
        private final ReentrantLock offsetUpdateLock;
        private final Condition offsetReachedThreshold;
        private Consumer<byte[], byte[]> consumer;
        private long offsetInSchemasTopic = -1L;
        // Noop key is only used to help reliably determine last offset; reader thread ignores
        // messages with this key
        private final K noopKey;

        private Properties consumerProps = new Properties();

        public WorkerThread(String bootstrapBrokers,
                                      String topic,
                                      String groupId,
                                      CacheUpdateHandler<K, V> cacheUpdateHandler,
                                      Serde<K> keySerde,
                                      Serde<V> valueSerde,
                                      Cache<K, V> localCache,
                                      K noopKey,
                                      CacheConfig config) {
            super("kafka-store-reader-thread-" + topic);  // this thread is not interruptible
            offsetUpdateLock = new ReentrantLock();
            offsetReachedThreshold = offsetUpdateLock.newCondition();
            this.topic = topic;
            this.groupId = groupId;
            this.cacheUpdateHandler = cacheUpdateHandler;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.localCache = localCache;
            this.noopKey = noopKey;

            addSchemaRegistryConfigsToClientProperties(config, consumerProps);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
            consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaStore-reader-" + this.topic);

            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

            log.info("Kafka store reader thread starting consumer");
            this.consumer = new KafkaConsumer<>(consumerProps);

            // Include a few retries since topic creation may take some time to propagate and schema
            // registry is often started immediately after creating the schemas topic.
            int retries = 0;
            List<PartitionInfo> partitions = null;
            while (retries++ < 10) {
                partitions = this.consumer.partitionsFor(this.topic);
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
                    + " backing this data store. Topic may not exist.");
            } else if (partitions.size() > 1) {
                throw new IllegalStateException("Unexpected number of partitions in the "
                    + topic
                    + " topic. Expected 1 and instead got " + partitions.size());
            }

            this.topicPartition = new TopicPartition(topic, 0);
            this.consumer.assign(Arrays.asList(this.topicPartition));
            this.consumer.seekToBeginning(Arrays.asList(this.topicPartition));

            log.info("Initialized last consumed offset to " + offsetInSchemasTopic);

            log.debug("Kafka store reader thread started");
        }

        @Override
        public void run() {
            try {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    K messageKey = null;
                    try {
                        messageKey = this.keySerde.deserializer().deserialize(topic, record.key());
                    } catch (Exception e) {
                        log.error("Failed to deserialize the schema or config key", e);
                        continue;
                    }

                    if (messageKey.equals(noopKey)) {
                        // If it's a noop, update local offset counter and do nothing else
                        try {
                            offsetUpdateLock.lock();
                            offsetInSchemasTopic = record.offset();
                            offsetReachedThreshold.signalAll();
                        } finally {
                            offsetUpdateLock.unlock();
                        }
                    } else {
                        V message = null;
                        try {
                            message =
                                record.value() == null ? null
                                    : valueSerde.deserializer().deserialize(topic, record.value());
                        } catch (Exception e) {
                            log.error("Failed to deserialize a schema or config update", e);
                            continue;
                        }
                        try {
                            log.trace("Applying update ("
                                + messageKey
                                + ","
                                + message
                                + ") to the local store");
                            if (message == null) {
                                localCache.remove(messageKey);
                            } else {
                                localCache.put(messageKey, message);
                            }
                            this.cacheUpdateHandler.handleUpdate(messageKey, message);
                            try {
                                offsetUpdateLock.lock();
                                offsetInSchemasTopic = record.offset();
                                offsetReachedThreshold.signalAll();
                            } finally {
                                offsetUpdateLock.unlock();
                            }
                        } catch (Exception se) {
                            log.error("Failed to add record from the Kafka topic"
                                + topic
                                + " the local store");
                        }
                    }
                }
            } catch (WakeupException we) {
                // do nothing because the thread is closing -- see shutdown()
            } catch (RecordTooLargeException rtle) {
                throw new IllegalStateException(
                    "Consumer threw RecordTooLargeException. A schema has been written that "
                        + "exceeds the default maximum fetch size.", rtle);
            } catch (RuntimeException e) {
                log.error("KafkaStoreReader thread has died for an unknown reason.");
                throw new RuntimeException(e);
            }
        }

        /*
        @Override
        public void shutdown() {
            log.debug("Starting shutdown of KafkaStoreReaderThread.");

            super.initiateShutdown();
            if (consumer != null) {
                consumer.wakeup();
            }
            if (localCache != null) {
                localCache.close();
            }
            super.awaitShutdown();
            consumer.close();
            log.info("KafkaStoreReaderThread shutdown complete.");
        }
        */

        public void waitUntilOffset(long offset, long timeout, TimeUnit timeUnit) throws CacheException {
            if (offset < 0) {
                throw new CacheException("KafkaStoreReaderThread can't wait for a negative offset.");
            }

            log.trace("Waiting to read offset {}. Currently at offset {}", offset, offsetInSchemasTopic);

            try {
                offsetUpdateLock.lock();
                long timeoutNs = TimeUnit.NANOSECONDS.convert(timeout, timeUnit);
                while ((offsetInSchemasTopic < offset) && (timeoutNs > 0)) {
                    try {
                        timeoutNs = offsetReachedThreshold.awaitNanos(timeoutNs);
                    } catch (InterruptedException e) {
                        log.debug("Interrupted while waiting for the background store reader thread to reach"
                            + " the specified offset: " + offset, e);
                    }
                }
            } finally {
                offsetUpdateLock.unlock();
            }

            if (offsetInSchemasTopic < offset) {
                throw new CacheTimeoutException(
                    "KafkaStoreReaderThread failed to reach target offset within the timeout interval. "
                        + "targetOffset: " + offset + ", offsetReached: " + offsetInSchemasTopic
                        + ", timeout(ms): " + TimeUnit.MILLISECONDS.convert(timeout, timeUnit));
            }
        }

        /* for testing purposes */
        /*
        public String getConsumerProperty(String key) {
            return this.consumerProps.getProperty(key);
        }
        */
    }
}
