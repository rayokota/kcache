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

import io.kcache.CacheUpdateHandler.ValidationStatus;
import io.kcache.KafkaCacheConfig.Offset;
import io.kcache.exceptions.CacheException;
import io.kcache.exceptions.CacheInitializationException;
import io.kcache.exceptions.CacheTimeoutException;
import io.kcache.exceptions.EntryTooLargeException;
import io.kcache.utils.InMemoryBoundedCache;
import io.kcache.utils.InMemoryCache;
import io.kcache.utils.ShutdownableThread;
import io.kcache.utils.OffsetCheckpoint;
import java.lang.reflect.Constructor;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class KafkaCache<K, V> implements Cache<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaCache.class);

    private String topic;
    private int desiredReplicationFactor;
    private int desiredNumPartitions;
    private List<Integer> partitions;
    private Offset offset;
    private String groupId;
    private String clientId;
    private CacheUpdateHandler<K, V> cacheUpdateHandler;
    private Serde<K> keySerde;
    private Serde<V> valueSerde;
    private Cache<K, V> localCache;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private boolean skipValidation;
    private boolean requireCompact;
    private boolean readOnly;
    private int initTimeout;
    private int timeout;
    private String checkpointDir;
    private int checkpointVersion;
    private String bootstrapBrokers;
    private Producer<byte[], byte[]> producer;
    private Partitioner partitioner;
    private Consumer<byte[], byte[]> consumer;
    private WorkerThread kafkaTopicReader;
    private KafkaCacheConfig config;
    private OffsetCheckpoint checkpointFile;
    private final Map<TopicPartition, Long> checkpointFileCache = new HashMap<>();
    private final Map<Integer, Long> lastWrittenOffsets = new ConcurrentHashMap<>();

    public KafkaCache(String bootstrapServers,
                      Serde<K> keySerde,
                      Serde<V> valueSerde) {
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        setUp(new KafkaCacheConfig(props), keySerde, valueSerde, null, null, null, null);
    }

    public KafkaCache(KafkaCacheConfig config,
                      Serde<K> keySerde,
                      Serde<V> valueSerde) {
        setUp(config, keySerde, valueSerde, null, null, null, null);
    }

    public KafkaCache(KafkaCacheConfig config,
                      Serde<K> keySerde,
                      Serde<V> valueSerde,
                      CacheUpdateHandler<K, V> cacheUpdateHandler,
                      Cache<K, V> localCache) {
        setUp(config, keySerde, valueSerde, cacheUpdateHandler, null, null, localCache);
    }

    public KafkaCache(KafkaCacheConfig config,
                      Serde<K> keySerde,
                      Serde<V> valueSerde,
                      CacheUpdateHandler<K, V> cacheUpdateHandler,
                      String backingCacheName,
                      Comparator<K> comparator) {
        setUp(config, keySerde, valueSerde, cacheUpdateHandler, backingCacheName, comparator, null);
    }

    private void setUp(KafkaCacheConfig config,
                       Serde<K> keySerde,
                       Serde<V> valueSerde,
                       CacheUpdateHandler<K, V> cacheUpdateHandler,
                       String backingCacheName,
                       Comparator<K> comparator,
                       Cache<K, V> localCache) {
        this.config = config;
        this.topic = config.getString(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG);
        this.desiredReplicationFactor = config.getInt(KafkaCacheConfig.KAFKACACHE_TOPIC_REPLICATION_FACTOR_CONFIG);
        this.desiredNumPartitions = config.getInt(KafkaCacheConfig.KAFKACACHE_TOPIC_NUM_PARTITIONS_CONFIG);
        this.partitions = config.partitions();
        this.offset = config.offset();
        this.groupId = config.getString(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG);
        this.clientId = config.getString(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG);
        if (this.clientId == null) {
            this.clientId = "kafka-cache-reader-" + this.topic;
        }
        this.skipValidation = config.getBoolean(KafkaCacheConfig.KAFKACACHE_TOPIC_SKIP_VALIDATION_CONFIG);
        this.requireCompact = config.getBoolean(KafkaCacheConfig.KAFKACACHE_TOPIC_REQUIRE_COMPACT_CONFIG);
        this.readOnly = config.getBoolean(KafkaCacheConfig.KAFKACACHE_TOPIC_READ_ONLY_CONFIG);
        this.initTimeout = config.getInt(KafkaCacheConfig.KAFKACACHE_INIT_TIMEOUT_CONFIG);
        this.timeout = config.getInt(KafkaCacheConfig.KAFKACACHE_TIMEOUT_CONFIG);
        this.checkpointDir = config.getString(KafkaCacheConfig.KAFKACACHE_CHECKPOINT_DIR_CONFIG);
        this.checkpointVersion = config.getInt(KafkaCacheConfig.KAFKACACHE_CHECKPOINT_VERSION_CONFIG);
        this.cacheUpdateHandler =
            cacheUpdateHandler != null ? cacheUpdateHandler : (key, value, oldValue, tp, offset, ts) -> {};
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.localCache = localCache != null ? localCache : createLocalCache(backingCacheName, comparator);
        this.bootstrapBrokers = config.bootstrapBrokers();

        log.info("Initializing Kafka cache {} with broker endpoints {}", clientId, bootstrapBrokers);
    }

    @SuppressWarnings("unchecked")
    private Cache<K, V> createLocalCache(String backingCacheName, Comparator<K> cmp) {
        try {
            if (backingCacheName == null) {
                backingCacheName = "default";
            }
            CacheType cacheType = CacheType.get(
                config.getString(KafkaCacheConfig.KAFKACACHE_BACKING_CACHE_CONFIG));
            int maxSize = config.getInt(KafkaCacheConfig.KAFKACACHE_BOUNDED_CACHE_SIZE_CONFIG);
            int expiry = config.getInt(KafkaCacheConfig.KAFKACACHE_BOUNDED_CACHE_EXPIRY_SECS_CONFIG);
            String clsName = null;
            boolean isPersistent = false;
            switch (cacheType) {
                case MEMORY:
                    return maxSize >= 0 || expiry >= 0
                        ? new InMemoryBoundedCache<>(maxSize, Duration.ofSeconds(expiry), null, cmp)
                        : new InMemoryCache<>(cmp);
                case BDBJE:
                    clsName = "io.kcache.bdbje.BdbJECache";
                    isPersistent = true;
                    break;
                case CAFFEINE:
                    clsName = "io.kcache.caffeine.CaffeineCache";
                    break;
                case LMDB:
                    clsName = "io.kcache.lmdb.LmdbCache";
                    isPersistent = true;
                    break;
                case MAPDB:
                    clsName = "io.kcache.mapdb.MapDBCache";
                    isPersistent = true;
                    break;
                case RDBMS:
                    clsName = "io.kcache.rdbms.RdbmsCache";
                    isPersistent = true;
                    break;
                case ROCKSDB:
                    clsName = "io.kcache.rocksdb.RocksDBCache";
                    isPersistent = true;
                    break;
            }
            Class<? extends Cache<K, V>> cls = (Class<? extends Cache<K, V>>) Class
                .forName(clsName);
            Cache<K, V> cache;
            if (isPersistent) {
                String dataDir = config.getString(KafkaCacheConfig.KAFKACACHE_DATA_DIR_CONFIG);
                Constructor<? extends Cache<K, V>> ctor = cls.getConstructor(
                    String.class, String.class, Serde.class, Serde.class, Comparator.class);
                cache = ctor.newInstance(backingCacheName, dataDir, keySerde, valueSerde, cmp);
            } else {
                Constructor<? extends Cache<K, V>> ctor = cls.getConstructor(
                    Integer.class, Duration.class, CacheLoader.class, Comparator.class);
                cache = ctor.newInstance(maxSize, Duration.ofSeconds(expiry), null, cmp);
            }
            Map<String, ?> configs = config.originalsWithPrefix(
                KafkaCacheConfig.KAFKACACHE_BACKING_CACHE_CONFIG + "." + cacheType + ".");
            cache.configure(configs);
            return cache;
        } catch (Exception e) {
            throw new CacheInitializationException("Could not create backing cache", e);
        }
    }

    @Override
    public Comparator<? super K> comparator() {
        return localCache.comparator();
    }

    @Override
    public boolean isPersistent() {
        return localCache.isPersistent();
    }

    @Override
    public void init() throws CacheInitializationException {
        if (initialized.get()) {
            throw new CacheInitializationException(
                "Illegal state while initializing cache for " + clientId + ". Cache was already initialized");
        }

        if (localCache.isPersistent()) {
            try {
                checkpointFile = new OffsetCheckpoint(checkpointDir, checkpointVersion, topic);
                checkpointFileCache.putAll(checkpointFile.read());
            } catch (IOException e) {
                throw new CacheInitializationException("Failed to read checkpoints", e);
            }
            log.info("Successfully read checkpoints");
        }
        localCache.init();

        if (!skipValidation) {
            createOrVerifyTopic();
        }
        this.consumer = new KafkaConsumer<>(getConsumerProperties());
        if (!readOnly) {
            Properties producerProperties = getProducerProperties();
            this.producer = new KafkaProducer<>(producerProperties);
            ProducerConfig producerConfig = new ProducerConfig(producerProperties);
            this.partitioner = producerConfig.getConfiguredInstance(
                ProducerConfig.PARTITIONER_CLASS_CONFIG,
                Partitioner.class,
                Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId));
        }

        // start the background thread that subscribes to the Kafka topic and applies updates.
        // the thread must be created after the topic has been created.
        this.kafkaTopicReader = new WorkerThread();
        try {
            this.kafkaTopicReader.readToEndOffsets(Duration.ofMillis(initTimeout));
        } catch (IOException e) {
            throw new CacheInitializationException("Failed to read to end offsets", e);
        }
        this.kafkaTopicReader.start();

        boolean isInitialized = initialized.compareAndSet(false, true);
        if (!isInitialized) {
            throw new CacheInitializationException("Illegal state while initializing cache for " + clientId
                + ". Cache was already initialized");
        }
        this.cacheUpdateHandler.cacheInitialized(new HashMap<>(checkpointFileCache));
    }

    @Override
    public void reset() {
        assertInitialized();
        lastWrittenOffsets.clear();
        localCache.reset();
    }

    @Override
    public void sync() {
        assertInitialized();
        kafkaTopicReader.waitUntilEndOffsets(Duration.ofMillis(timeout));
        localCache.sync();
    }

    private Properties getConsumerProperties() {
        Properties consumerProps = new Properties();
        addKafkaCacheConfigsToClientProperties(consumerProps);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        return consumerProps;
    }

    private Properties getProducerProperties() {
        Properties producerProps = new Properties();
        addKafkaCacheConfigsToClientProperties(producerProps);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0); // Producer should not retry
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        return producerProps;
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
            } else if (!readOnly){
                createTopic(admin);
            } else {
                throw new CacheInitializationException("Topic does not exist " + topic + " and cache is configured read-only");
            }
        } catch (TimeoutException e) {
            throw new CacheInitializationException(
                "Timed out trying to create or validate topic " + topic,
                e
            );
        } catch (InterruptedException | ExecutionException e) {
            throw new CacheInitializationException(
                "Failed trying to create or validate topic " + topic,
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

        NewTopic topicRequest = new NewTopic(topic, desiredNumPartitions, (short) topicReplicationFactor);
        Map<String, String> topicConfigs = new HashMap(config.originalsWithPrefix("kafkacache.topic.config."));
        topicConfigs.put(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.CLEANUP_POLICY_COMPACT
        );
        topicRequest.configs(topicConfigs);
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
            topicDescription = admin.describeTopics(topics).allTopicNames().get(initTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                log.warn("Could not validate existing topic {}", topic);
                return;
            } else {
                throw e;
            }
        }

        TopicDescription description = topicDescription.get(topic);
        final int numPartitions = description.partitions().size();
        if (numPartitions < desiredNumPartitions) {
            log.warn("The number of partitions for the topic "
                + topic
                + " is less than the desired value of "
                + desiredReplicationFactor
                + ".");
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
            String message = "The retention policy of the topic " + topic + " is not 'compact'. "
                + "You must configure the topic to 'compact' cleanup policy to avoid Kafka "
                + "deleting your data after a week. "
                + "Refer to Kafka documentation for more details on cleanup policies.";
            if (requireCompact) {
                log.error(message);
                throw new CacheInitializationException("The retention policy of the topic " + topic
                    + " is incorrect. Expected cleanup.policy to be "
                    + "'compact' but it is " + retentionPolicy);
            } else {
                log.warn(message);
            }
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
        return put(null, key, value).getOldValue();
    }

    public Metadata<V> put(Headers headers, K key, V value) {
        if (readOnly) {
            throw new CacheException("Cache is read-only");
        }

        assertInitialized();
        V oldValue = key != null ? get(key) : null;

        RecordMetadata recordMetadata = doPut(() -> {
            // write to the Kafka topic
            ProducerRecord<byte[], byte[]> producerRecord = toRecord(headers, key, value);
            log.trace("Sending record to Kafka cache topic: {}", producerRecord);
            return producer.send(producerRecord);
        });

        return new Metadata<>(recordMetadata, oldValue);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        if (readOnly) {
            throw new CacheException("Cache is read-only");
        }

        assertInitialized();
        if (entries.isEmpty()) {
            return;
        }

        doPut(() -> {
            Future<RecordMetadata> ack = null;
            for (Map.Entry<? extends K, ? extends V> entry : entries.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();

                // write to the Kafka topic
                ProducerRecord<byte[], byte[]> producerRecord = toRecord(null, key, value);
                log.trace("Sending record to Kafka cache topic: {}", producerRecord);
                ack = producer.send(producerRecord);
            }
            producer.flush();
            // Return last ack
            return ack;
        });
    }

    private RecordMetadata doPut(Supplier<Future<RecordMetadata>> ackSupplier) {
        RecordMetadata recordMetadata;
        Integer lastWrittenPartition = null;
        Long previousWrittenOffset = null;
        boolean knownSuccessfulWrite = false;
        try {
            Future<RecordMetadata> ack = ackSupplier.get();
            recordMetadata = ack.get(timeout, TimeUnit.MILLISECONDS);

            log.trace("Waiting for the local cache to catch up to offset {}", recordMetadata.offset());
            lastWrittenPartition = recordMetadata.partition();
            long lastWrittenOffset = recordMetadata.offset();
            previousWrittenOffset = lastWrittenOffsets.put(lastWrittenPartition, lastWrittenOffset);
            kafkaTopicReader.waitUntilOffset(lastWrittenPartition, lastWrittenOffset, Duration.ofMillis(timeout));
            knownSuccessfulWrite = true;
        } catch (InterruptedException e) {
            throw new CacheException("Put operation interrupted while waiting for an ack from Kafka", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RecordTooLargeException) {
                throw new EntryTooLargeException("Put operation failed because entry is too large");
            } else {
                throw new CacheException("Put operation failed while waiting for an ack from Kafka", e);
            }
        } catch (TimeoutException e) {
            throw new CacheTimeoutException(
                "Put operation timed out while waiting for an ack from Kafka", e);
        } catch (KafkaException ke) {
            throw new CacheException("Put operation to Kafka failed", ke);
        } finally {
            if (!knownSuccessfulWrite && lastWrittenPartition != null) {
                if (previousWrittenOffset != null) {
                    lastWrittenOffsets.put(lastWrittenPartition, previousWrittenOffset);
                } else {
                    lastWrittenOffsets.remove(lastWrittenPartition);
                }
            }
        }
        return recordMetadata;
    }

    private ProducerRecord<byte[], byte[]> toRecord(Headers headers, K key, V value) {
        ProducerRecord<byte[], byte[]> producerRecord;
        try {
            byte[] keyBytes = key == null
                ? null : this.keySerde.serializer().serialize(topic, headers, key);
            byte[] valueBytes = value == null
                ? null : this.valueSerde.serializer().serialize(topic, headers, value);
            producerRecord =
                new ProducerRecord<>(
                    topic,
                    partition(topic, key, keyBytes, value, valueBytes),
                    keyBytes,
                    valueBytes,
                    headers
                );
        } catch (Exception e) {
            throw new CacheException("Error serializing key while creating the Kafka produce record", e);
        }
        return producerRecord;
    }

    private Integer partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes) {
        if (partitioner == null || partitioner instanceof DefaultPartitioner) {
            return null;
        }
        try {
            int customPartition = partitioner.partition(topic, key, keyBytes, value, valueBytes,
                null);
            if (customPartition < 0) {
                throw new IllegalArgumentException(String.format(
                    "The partitioner generated an invalid partition number: %d. "
                        + "Partition number should always be non-negative.", customPartition));
            }
            return customPartition;
        } catch (Exception e) {
            log.warn("Could not invoke partitioner", e);
            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        if (readOnly) {
            throw new CacheException("Cache is read-only");
        }
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
        return readOnly ? Collections.unmodifiableSet(localCache.keySet()) : localCache.keySet();
    }

    @Override
    public Collection<V> values() {
        assertInitialized();
        return readOnly ? Collections.unmodifiableCollection(localCache.values()) : localCache.values();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        assertInitialized();
        return readOnly ? Collections.unmodifiableSet(localCache.entrySet()) : localCache.entrySet();
    }

    @Override
    public K firstKey() {
        assertInitialized();
        return localCache.firstKey();
    }

    @Override
    public K lastKey() {
        assertInitialized();
        return localCache.lastKey();
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
    public Cache<K, V> descendingCache() {
        assertInitialized();
        return localCache.descendingCache();
    }

    @Override
    public void flush() {
        assertInitialized();
        if (producer != null) {
            producer.flush();
        }
        localCache.flush();
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
            log.info("Kafka cache producer shut down for {}", clientId);
        }
        localCache.close();
        if (checkpointFile != null) {
            checkpointFile.close();
        }
        if (cacheUpdateHandler != null) {
            cacheUpdateHandler.close();
        }
        log.info("Kafka cache shut down complete for {}", clientId);
    }

    @Override
    public void destroy() throws IOException {
        assertInitialized();
        localCache.destroy();
    }

    private void assertInitialized() throws CacheException {
        if (!initialized.get()) {
            throw new CacheException("Illegal state. Cache for " + clientId + " not initialized yet");
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
     * <p>On startup, this thread will always read from a specified start of the topic. We assume
     * the topic will always be small, hence the startup time to read the topic won't take
     * too long. Because the topic is always read from a specified start, the consumer never
     * commits offsets.
     */
    private class WorkerThread extends ShutdownableThread {

        private final ReentrantLock consumerLock;
        private final Condition runningCondition;
        private final AtomicBoolean isRunning;
        private final ReentrantLock offsetUpdateLock;
        private final Condition offsetReachedThreshold;
        private final Map<Integer, Long> lastReadOffsets = new ConcurrentHashMap<>();

        public WorkerThread() {
            super("kafka-cache-reader-thread-" + topic);
            consumerLock = new ReentrantLock();
            runningCondition = consumerLock.newCondition();
            isRunning = new AtomicBoolean(true);
            offsetUpdateLock = new ReentrantLock();
            offsetReachedThreshold = offsetUpdateLock.newCondition();

            if (partitions.isEmpty()) {
                // Include a few retries since topic creation may take some time to propagate and
                // cache is often started immediately after creating the topic.
                int retries = 0;
                List<PartitionInfo> partitionInfos;
                while (retries++ < 10) {
                    partitionInfos = consumer.partitionsFor(topic, Duration.ofMillis(initTimeout));
                    if (partitionInfos != null && !partitionInfos.isEmpty()) {
                        partitions = partitionInfos.stream()
                            .map(PartitionInfo::partition)
                            .collect(Collectors.toList());
                        break;
                    }

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                if (partitions.isEmpty()) {
                    throw new IllegalArgumentException("Unable to subscribe to the Kafka topic "
                        + topic
                        + " backing this data cache. Topic may not exist.");
                }
            }

            List<TopicPartition> topicPartitions = partitions.stream()
                .peek(p -> lastReadOffsets.put(p, -1L))
                .map(p -> new TopicPartition(topic, p))
                .collect(Collectors.toList());
            consumer.assign(topicPartitions);

            if (localCache.isPersistent()) {
                for (final TopicPartition topicPartition : topicPartitions) {
                    final Long checkpoint = checkpointFileCache.get(topicPartition);
                    if (checkpoint != null) {
                        log.info("Seeking to checkpoint {} for {}", checkpoint, topicPartition);
                        consumer.seek(topicPartition, checkpoint);
                    } else {
                        log.info("Seeking to start for {}", topicPartition);
                        seekToStart(Collections.singleton(topicPartition), Duration.ofMillis(initTimeout));
                    }
                }
            } else {
                log.info("Seeking to start for all partitions for topic {}", topic);
                seekToStart(topicPartitions, Duration.ofMillis(initTimeout));
            }

            log.info("Initialized last read offsets to {}", lastReadOffsets);

            log.info("KafkaTopicReader thread started for {}.", clientId);
        }

        private void readToEndOffsets(Duration timeout) throws IOException {
            Set<TopicPartition> assignment = consumer.assignment();
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment, timeout);
            log.info("Reading to end of offsets {}", endOffsets);

            int count = 0;
            while (!hasReadToEndOffsets(endOffsets, timeout)) {
                try {
                    count += poll();
                } catch (InvalidOffsetException e) {
                    if (localCache.isPersistent()) {
                        localCache.close();
                        localCache.destroy();
                        localCache.init();
                    }
                    log.warn("Seeking to start due to invalid offset", e);
                    seekToStart(assignment, timeout);
                    count = 0;
                }
            }
            log.info("During init or sync, processed {} records from topic {}", count, topic);
        }

        private void seekToStart(Collection<TopicPartition> topicPartitions, Duration timeout) {
            switch (offset.getOffsetType()) {
                case BEGINNING:
                    consumer.seekToBeginning(topicPartitions);
                    break;
                case END:
                    consumer.seekToEnd(topicPartitions);
                    break;
                case ABSOLUTE:
                    for (TopicPartition tp : topicPartitions) {
                        consumer.seek(tp, offset.getOffset());
                    }
                    break;
                case RELATIVE:
                    Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions, timeout);
                    for (TopicPartition tp : topicPartitions) {
                        consumer.seek(tp, Math.max(endOffsets.get(tp) - offset.getOffset(), 0));
                    }
                    break;
                case TIMESTAMP:
                    Map<TopicPartition, Long> timestamps = topicPartitions.stream()
                        .collect(Collectors.toMap(tp -> tp, tp -> offset.getOffset()));
                    Map<TopicPartition, OffsetAndTimestamp> offsets = null;
                    try {
                        offsets = consumer.offsetsForTimes(timestamps, timeout);
                    } catch (KafkaException e) {
                        log.warn("Could not fetch offset times for topic {}", topic, e);
                    }
                    for (TopicPartition tp : topicPartitions) {
                        if (offsets != null && offsets.get(tp) != null) {
                            consumer.seek(tp, offsets.get(tp).offset());
                        } else {
                            consumer.seekToBeginning(Collections.singleton(tp));
                            log.warn("Could not find offset time for topic {}, partition {}, ts {}, "
                                + "seeking to beginning", tp.topic(), tp.partition(), offset.getOffset());
                        }
                    }
                    break;
            }
        }

        private boolean hasReadToEndOffsets(Map<TopicPartition, Long> endOffsets, Duration timeout) {
            endOffsets.entrySet().removeIf(entry ->
                consumer.position(entry.getKey(), timeout) >= entry.getValue());
            return endOffsets.isEmpty();
        }

        @Override
        protected void doWork() {
            try {
                consumerLock.lock();
                while (!isRunning.get()) {
                    runningCondition.await();
                }
                poll();
            } catch (InterruptedException e) {
                // ignore
            } finally {
                consumerLock.unlock();
            }
        }

        private int poll() {
            int count = 0;
            try {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                count = records.count();
                cacheUpdateHandler.startBatch(count);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    try {
                        K messageKey;
                        try {
                            messageKey = keySerde.deserializer().deserialize(topic, record.headers(), record.key());
                        } catch (Exception e) {
                            log.error("Failed to deserialize the key", e);
                            continue;
                        }

                        V message;
                        try {
                            message =
                                record.value() == null ? null
                                    : valueSerde.deserializer().deserialize(topic, record.headers(), record.value());
                        } catch (Exception e) {
                            log.error("Failed to deserialize a value", e);
                            continue;
                        }
                        Headers headers = record.headers();
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        long offset = record.offset();
                        long timestamp = record.timestamp();
                        TimestampType tsType = record.timestampType();
                        Optional<Integer> leaderEpoch = record.leaderEpoch();
                        ValidationStatus status =
                            cacheUpdateHandler.validateUpdate(headers, messageKey, message,
                                tp, offset, timestamp, tsType, leaderEpoch);
                        V oldMessage = null;
                        switch (status) {
                            case SUCCESS:
                                if (messageKey != null) {
                                    log.trace("Applying update ({}, {}) to the local cache", messageKey, message);
                                    if (message == null) {
                                        oldMessage = localCache.remove(messageKey);
                                    } else {
                                        oldMessage = localCache.put(messageKey, message);
                                    }
                                }
                                cacheUpdateHandler.handleUpdate(headers, messageKey, message,
                                    oldMessage, tp, offset, timestamp, tsType, leaderEpoch);
                                break;
                            case ROLLBACK_FAILURE:
                                if (readOnly || messageKey == null) {
                                    log.warn("Ignore invalid update to key {}", messageKey);
                                    break;
                                }
                                oldMessage = localCache.get(messageKey);
                                if (!Objects.equals(message, oldMessage)) {
                                    try {
                                        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
                                            record.topic(),
                                            record.partition(),
                                            record.key(),
                                            oldMessage == null ? null
                                                : valueSerde.serializer().serialize(topic, headers, oldMessage),
                                            headers
                                        );
                                        producer.send(producerRecord);
                                        log.warn("Rollback invalid update to key {}", messageKey);
                                    } catch (KafkaException ke) {
                                        log.error("Failed to rollback invalid update to key {}",
                                            messageKey, ke);
                                    }
                                }
                                break;
                            case IGNORE_FAILURE:
                                log.warn("Ignore invalid update to key {}", messageKey);
                                break;
                        }
                    } catch (Exception se) {
                        log.error("Failed to add record from the Kafka topic "
                            + topic
                            + " to the local cache", se);
                    } finally {
                        updateOffset(record.partition(), record.offset());
                    }
                }
                if (localCache.isPersistent() && initialized.get()) {
                    try {
                        localCache.flush();
                        Map<TopicPartition, Long> offsets = cacheUpdateHandler.checkpoint(count);
                        checkpointOffsets(offsets);
                    } catch (CacheException e) {
                        log.warn("Failed to flush", e);
                    }
                }
                cacheUpdateHandler.endBatch(count);
            } catch (Throwable t) {
                cacheUpdateHandler.failBatch(count, t);
                if (t instanceof WakeupException) {
                    // do nothing
                } else if (t instanceof RecordTooLargeException) {
                    throw new IllegalStateException(
                        "Consumer threw RecordTooLargeException. Data has been written that "
                            + "exceeds the default maximum fetch size.", t);
                } else {
                    log.error("KafkaTopicReader thread for {} has died for an unknown reason.", clientId, t);
                    throw t;
                }
            }
            return count;
        }

        private void updateOffset(int partition, long offset) {
            try {
                offsetUpdateLock.lock();
                lastReadOffsets.put(partition, offset);
                offsetReachedThreshold.signalAll();
            } finally {
                offsetUpdateLock.unlock();
            }
        }

        private void checkpointOffsets(Map<TopicPartition, Long> offsets) {
            Map<TopicPartition, Long> newOffsets = offsets != null
                ? offsets
                : lastReadOffsets.entrySet().stream()
                    .collect(Collectors.toMap(e -> new TopicPartition(topic, e.getKey()), e -> e.getValue() + 1));
            checkpointFileCache.putAll(newOffsets);
            try {
                checkpointFile.write(checkpointFileCache);
            } catch (final IOException e) {
                log.warn("Failed to write offset checkpoint file to {}: {}", checkpointFile, e);
            }
        }

        private void waitUntilOffset(int partition, long offset, Duration timeout) throws CacheException {
            if (offset < 0) {
                throw new CacheException("KafkaTopicReader thread can't wait for a negative offset.");
            }

            log.trace("Waiting to read offset {}. Currently at offset {}", offset, lastReadOffsets.get(partition));

            try {
                offsetUpdateLock.lock();
                long timeoutNs = timeout.toNanos();
                while (lastReadOffsets.get(partition) < offset && timeoutNs > 0) {
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

            if (lastReadOffsets.get(partition) < offset) {
                throw new CacheTimeoutException(
                    "KafkaCacheTopic thread failed to reach target offset within the timeout interval. "
                        + "targetOffset: " + offset + ", offsetReached: " + lastReadOffsets.get(partition)
                        + ", timeout(ms): " + timeout.toMillis());
            }
        }

        private void waitUntilEndOffsets(Duration timeout) throws CacheException {
            Map<Integer, Long> lastOffsets = new HashMap<>(lastWrittenOffsets);
            // Optimization in case of writes
            if (hasValidLastWrittenOffsets(lastOffsets)) {
                if (hasReadToLastWrittenOffsets(lastOffsets)) {
                    return;
                }
                try {
                    offsetUpdateLock.lock();
                    long timeoutNs = timeout.toNanos();
                    while (!hasReadToLastWrittenOffsets(lastOffsets) && timeoutNs > 0) {
                        try {
                            timeoutNs = offsetReachedThreshold.awaitNanos(timeoutNs);
                        } catch (InterruptedException e) {
                            log.debug(
                                "Interrupted while waiting for the background cache reader thread to reach"
                                    + " the end offsets", e);
                        }
                    }
                } finally {
                    offsetUpdateLock.unlock();
                }

                if (hasReadToLastWrittenOffsets(lastOffsets)) {
                    return;
                } else {
                    log.warn("Could not read to last written offsets {}", lastOffsets);
                }
            }
            waitUntilConsumerEndOffsets(timeout);
        }

        private boolean hasValidLastWrittenOffsets(Map<Integer, Long> lastOffsets) {
            return lastOffsets.keySet().containsAll(partitions);
        }

        private boolean hasReadToLastWrittenOffsets(Map<Integer, Long> lastOffsets) {
            return lastOffsets.entrySet().stream()
                .allMatch(entry -> {
                    int lastWrittenPartition = entry.getKey();
                    long lastWrittenOffset = entry.getValue();
                    long lastReadOffset = lastReadOffsets.getOrDefault(lastWrittenPartition, -1L);
                    return lastReadOffset >= lastWrittenOffset;
                });
        }

        private synchronized void waitUntilConsumerEndOffsets(Duration timeout) throws CacheException {
            isRunning.set(false);
            consumer.wakeup();
            try {
                consumerLock.lock();
                try {
                    readToEndOffsets(timeout);
                } catch (Exception e) {
                    log.warn("Could not read to end offsets", e);
                }
                isRunning.set(true);
                runningCondition.signalAll();
            } finally {
                consumerLock.unlock();
            }
        }

        @Override
        public void shutdown() throws InterruptedException {
            log.debug("Starting shutdown of KafkaTopicReader thread for {}.", clientId);

            super.initiateShutdown();
            if (consumer != null) {
                consumer.wakeup();
            }
            super.awaitShutdown();
            if (consumer != null) {
                consumer.close();
            }
            log.info("KafkaTopicReader thread shutdown complete for {}.", clientId);
        }
    }

    public static class Metadata<V> {
        private final RecordMetadata recordMetadata;
        private final V oldValue;

        public Metadata(RecordMetadata recordMetadata, V oldValue) {
            this.recordMetadata = recordMetadata;
            this.oldValue = oldValue;
        }

        public RecordMetadata getRecordMetadata() {
            return recordMetadata;
        }

        public V getOldValue() {
            return oldValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Metadata<?> metadata = (Metadata<?>) o;
            return Objects.equals(recordMetadata, metadata.recordMetadata)
                && Objects.equals(oldValue, metadata.oldValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(recordMetadata, oldValue);
        }

        @Override
        public String toString() {
            return "Metadata{" +
                "recordMetadata=" + recordMetadata +
                ", oldValue=" + oldValue +
                '}';
        }
    }
}
