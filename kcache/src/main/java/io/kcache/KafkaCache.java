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
import java.lang.reflect.Constructor;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
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
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
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
    // Guarded by this: whether init has succeeded, so that syncs go on, and why it has not, once
    // init has failed or the cache is closed first, so that the syncs waiting for it fail.
    private boolean initDone;
    private Throwable initFailure;
    private boolean skipValidation;
    private boolean requireCompact;
    private boolean readOnly;
    private int initTimeout;
    private int timeout;
    private long pollTimeout;
    private boolean checkpointBeforeInit;
    private CheckpointHandler checkpointHandler;
    private String bootstrapBrokers;
    private Producer<byte[], byte[]> producer;
    private Partitioner partitioner;
    private Consumer<byte[], byte[]> consumer;
    private WorkerThread kafkaTopicReader;
    private KafkaCacheConfig config;
    private final Map<Integer, Long> lastWrittenOffsets = new ConcurrentHashMap<>();
    private final Queue<CompletableFuture<Integer>> syncCallbacks = new ArrayDeque<>();
    // Guarded by this: once the reader thread has stopped, no sync is read for; and what it died
    // of, if it did, rather than stop as the cache closed. Written under the lock, but read
    // without it by syncs waiting for their own writes.
    private volatile boolean readerStopped;
    private Throwable readerFailure;

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
        this.pollTimeout = config.getLong(KafkaCacheConfig.KAFKACACHE_POLL_TIMEOUT_CONFIG);
        this.checkpointBeforeInit = config.getBoolean(KafkaCacheConfig.KAFKACACHE_CHECKPOINT_BEFORE_INIT_CONFIG);
        this.checkpointHandler = config.checkpointHandler();
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
        synchronized (this) {
            // A new init, as after one that failed: syncs wait for it, as for the first.
            initFailure = null;
        }
        try {
            initCache();
        } catch (RuntimeException | Error e) {
            discardFailedInit(e);
            synchronized (this) {
                initFailure = e;
                notifyAll();
            }
            throw e;
        }
    }

    /**
     * Drops what an init that failed applied to a local cache that is not persistent, so that an
     * init after it starts as a new cache would, rather than keep records that the topic, read
     * again, may no longer have. A persistent cache keeps them, as after a restart. Unless
     * another init has succeeded meanwhile, whose records they then are.
     */
    private void discardFailedInit(Throwable failure) {
        if (localCache.isPersistent() || initialized.get()) {
            return;
        }
        try {
            try {
                localCache.clear();
            } catch (UnsupportedOperationException e) {
                // As for a cache that guards its entries one by one.
                for (K key : new ArrayList<>(localCache.keySet())) {
                    localCache.remove(key);
                }
            }
            cacheUpdateHandler.cacheReset();
        } catch (RuntimeException e) {
            failure.addSuppressed(e);
        }
    }

    private void initCache() {
        if (localCache.isPersistent()) {
            checkpointHandler.init();
            log.info("Successfully read checkpoints");
        }
        localCache.init();

        if (!skipValidation) {
            createOrVerifyTopic();
        }
        this.consumer = new KafkaConsumer<>(getConsumerProperties());
        int count;
        try {
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
            try {
                // It finds the partitions and seeks to where it starts, either of which may time
                // out: a failure of init like any other.
                this.kafkaTopicReader = new WorkerThread();
            } catch (org.apache.kafka.common.errors.TimeoutException e) {
                throw new CacheInitializationException(
                    "Timed out trying to start reading topic " + topic, e);
            }
            try {
                count = this.kafkaTopicReader.readToEndOffsets(Duration.ofMillis(initTimeout));
            } catch (IOException | KafkaException e) {
                // Whatever the read failed with, as an error from a poll, it fails init.
                throw new CacheInitializationException("Failed to read to end offsets", e);
            }
        } catch (RuntimeException e) {
            // The clients are closed here, as the reader thread that would close the consumer
            // has not started; close() then has nothing of theirs left to wait for.
            closeClients();
            throw e;
        }
        Map<TopicPartition, Long> checkpoints = checkpointHandler.checkpoints();
        this.kafkaTopicReader.start();

        boolean isInitialized = initialized.compareAndSet(false, true);
        if (!isInitialized) {
            throw new CacheInitializationException("Illegal state while initializing cache for " + clientId
                + ". Cache was already initialized");
        }
        try {
            cacheUpdateHandler.cacheInitialized(count, checkpoints);
        } finally {
            // The cache is initialized, its reader running, even if the handler throws: syncs go
            // on, rather than fail for good on a cache that init cannot be tried again on.
            synchronized (this) {
                initDone = true;
                notifyAll();
            }
        }
    }

    @Override
    public void reset() {
        lastWrittenOffsets.clear();
        localCache.reset();
        cacheUpdateHandler.cacheReset();
    }

    @Override
    public void sync() {
        try {
            waitForInit();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CacheException(
                "Sync operation interrupted while waiting for cache initialization",
                e
            );
        }
        // Checked before either wait, as a cache that has written to every partition waits for
        // its own writes, which a stopped reader thread may already have read.
        synchronized (this) {
            if (readerStopped) {
                throw readerStoppedException();
            }
        }

        int count = -1;
        if (kafkaTopicReader != null) {
            count = kafkaTopicReader.waitUntilLastWrittenOffsets(Duration.ofMillis(timeout));
        }
        if (count < 0) {
            count = waitUntilEndOffsets();
        }
        localCache.sync();
        Map<TopicPartition, Long> checkpoints = checkpointHandler.checkpoints();
        cacheUpdateHandler.cacheSynchronized(count, checkpoints);
    }

    private int waitUntilEndOffsets() throws CacheException {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        synchronized (this) {
            if (readerStopped) {
                throw readerStoppedException();
            }
            syncCallbacks.add(future);
        }
        if (consumer != null) {
            consumer.wakeup();
        }
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CacheException("Sync operation interrupted while reading to end offsets", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof org.apache.kafka.common.errors.TimeoutException) {
                // As for put(): a timeout a caller may try again after.
                throw new CacheTimeoutException("Timed out reading to end offsets", cause);
            }
            throw new CacheException("Failed to read to end offsets", cause);
        }
    }

    // Guarded by this.
    private CacheException readerStoppedException() {
        return readerFailure != null
            ? new CacheException("The reader thread for " + clientId + " has died", readerFailure)
            : new CacheException(
                "The reader thread for " + clientId + " has stopped, as the cache was closed");
    }

    /**
     * Whether a read to end offsets moved between two takes of its positions. The later take
     * covers only the partitions still short of their end offsets: one that has reached its own
     * drops out of it, which is no move, so that a read stalled beside it times out after one
     * timeout rather than two.
     */
    static boolean hasMoved(Map<TopicPartition, Long> before, Map<TopicPartition, Long> now) {
        for (Map.Entry<TopicPartition, Long> entry : now.entrySet()) {
            Long position = before.get(entry.getKey());
            if (position == null || entry.getValue() > position) {
                return true;
            }
        }
        return false;
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
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

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
        Map<String, String> topicConfigs = config.originalsWithPrefix("kafkacache.topic.config.")
            .entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> e.getValue().toString()));
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
        return put(headers, key, value, false);
    }

    public Metadata<V> put(Headers headers, K key, V value, boolean flush) {
        if (readOnly) {
            throw new CacheException("Cache is read-only");
        }

        assertInitialized();
        V oldValue = key != null ? get(key) : null;

        RecordMetadata recordMetadata = doPut(() -> {
            // write to the Kafka topic
            ProducerRecord<byte[], byte[]> producerRecord = toRecord(headers, key, value);
            log.trace("Sending record to Kafka cache topic: {}", producerRecord);
            Future<RecordMetadata> ack = producer.send(producerRecord);
            if (flush) {
                producer.flush();
            }
            return Collections.singletonList(ack);
        });

        return new Metadata<>(recordMetadata, oldValue);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        putAll(null, entries);
    }

    public RecordMetadata putAll(Headers headers, Map<? extends K, ? extends V> entries) {
        return putAll(headers, entries, true);
    }

    public RecordMetadata putAll(Headers headers, Map<? extends K, ? extends V> entries, boolean flush) {
        if (readOnly) {
            throw new CacheException("Cache is read-only");
        }

        assertInitialized();
        if (entries.isEmpty()) {
            return null;
        }

        return doPut(() -> {
            List<Future<RecordMetadata>> acks = new ArrayList<>(entries.size());
            for (Map.Entry<? extends K, ? extends V> entry : entries.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();

                // write to the Kafka topic
                ProducerRecord<byte[], byte[]> producerRecord = toRecord(headers, key, value);
                log.trace("Sending record to Kafka cache topic: {}", producerRecord);
                acks.add(producer.send(producerRecord));
            }
            if (flush) {
                producer.flush();
            }
            return acks;
        });
    }

    private RecordMetadata doPut(Supplier<List<Future<RecordMetadata>>> acksSupplier) {
        RecordMetadata recordMetadata = null;
        // The highest offset written to each partition by this call
        Map<Integer, Long> writtenOffsets = new HashMap<>();
        // The offsets in lastWrittenOffsets that this call replaced, so that they can be rolled
        // back if the write is not known to have succeeded.  Only partitions whose offset this
        // call actually installed appear here; a null value means no offset was recorded for
        // that partition beforehand.
        Map<Integer, Long> replacedWrittenOffsets = new HashMap<>();
        boolean knownSuccessfulWrite = false;
        try {
            List<Future<RecordMetadata>> acks = acksSupplier.get();

            // Wait for an ack for every record, so that a record that fails is not masked by a
            // later one that succeeds.  The acks are awaited concurrently, so they share a
            // single timeout budget.
            long ackDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
            for (Future<RecordMetadata> ack : acks) {
                recordMetadata = ack.get(remainingNanos(ackDeadline), TimeUnit.NANOSECONDS);
                writtenOffsets.merge(recordMetadata.partition(), recordMetadata.offset(), Math::max);
            }

            log.trace("Waiting for the local cache to catch up to offsets {}", writtenOffsets);
            for (Map.Entry<Integer, Long> entry : writtenOffsets.entrySet()) {
                recordWrittenOffset(entry.getKey(), entry.getValue(), replacedWrittenOffsets);
            }
            // The local cache catches up to all partitions concurrently, so they too share a
            // single timeout budget.
            long catchUpDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
            for (Map.Entry<Integer, Long> entry : writtenOffsets.entrySet()) {
                kafkaTopicReader.waitUntilOffset(entry.getKey(), entry.getValue(),
                    Duration.ofNanos(remainingNanos(catchUpDeadline)));
            }
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
            if (!knownSuccessfulWrite) {
                for (Map.Entry<Integer, Long> entry : replacedWrittenOffsets.entrySet()) {
                    int partition = entry.getKey();
                    Long previousOffset = entry.getValue();
                    long installedOffset = writtenOffsets.get(partition);
                    // Only roll back while the offset installed by this call is still the one
                    // recorded, so that a concurrent write that has since advanced the partition
                    // is not clobbered
                    if (previousOffset != null) {
                        lastWrittenOffsets.replace(partition, installedOffset, previousOffset);
                    } else {
                        lastWrittenOffsets.remove(partition, installedOffset);
                    }
                }
            }
        }
        return recordMetadata;
    }

    /**
     * Records that this call wrote up to the given offset on the given partition, without
     * regressing a higher offset recorded by a concurrent write.  If the offset is installed,
     * the offset it replaced (possibly null) is added to {@code replacedWrittenOffsets} so that
     * it can be restored should the write turn out not to have succeeded.
     */
    private void recordWrittenOffset(int partition, long offset,
                                     Map<Integer, Long> replacedWrittenOffsets) {
        while (true) {
            Long previousOffset = lastWrittenOffsets.putIfAbsent(partition, offset);
            if (previousOffset == null) {
                replacedWrittenOffsets.put(partition, null);
                return;
            }
            if (previousOffset >= offset) {
                // A concurrent write has already recorded at least this offset
                return;
            }
            if (lastWrittenOffsets.replace(partition, previousOffset, offset)) {
                replacedWrittenOffsets.put(partition, previousOffset);
                return;
            }
        }
    }

    private static long remainingNanos(long deadlineNanos) {
        return Math.max(0L, deadlineNanos - System.nanoTime());
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
            int customPartition = partitioner.partition(topic, key, keyBytes, value, valueBytes, null);
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
        if (producer != null) {
            producer.flush();
        }
        localCache.flush();
        cacheUpdateHandler.cacheFlushed();
    }

    // Closes the Kafka clients after an init that failed before the reader thread started.
    private void closeClients() {
        closeConsumer();
        Utils.closeQuietly(producer, "Kafka cache producer for " + clientId);
        kafkaTopicReader = null;
        consumer = null;
        producer = null;
    }

    /**
     * Closes the consumer without waiting: it neither belongs to a group nor commits offsets, so
     * a timeout would only wait for its last fetch, which the broker holds on an idle topic for
     * up to fetch.max.wait.ms.
     */
    private void closeConsumer() {
        if (consumer != null) {
            Utils.closeQuietly(() -> consumer.close(Duration.ZERO),
                "Kafka cache consumer for " + clientId);
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (!initDone) {
                // No init follows that the syncs waiting for one could go on after.
                initFailure = new CacheException(
                    "The cache for " + clientId + " was closed before it was initialized");
                notifyAll();
            }
        }
        if (kafkaTopicReader != null) {
            kafkaTopicReader.shutdown();
        } else {
            closeConsumer();
        }
        Utils.closeQuietly(producer, "Kafka cache producer for " + clientId);
        localCache.close();
        checkpointHandler.close();
        cacheUpdateHandler.close();
        log.info("Kafka cache shut down complete for {}", clientId);
    }

    @Override
    public void destroy() throws IOException {
        localCache.destroy();
        cacheUpdateHandler.cacheDestroyed();
    }

    /**
     * Waits for the cache to be initialized, as by another thread.
     *
     * @throws CacheException if init fails, or the cache is closed, before the cache is
     *     initialized; a later init may still succeed
     */
    public void waitForInit() throws InterruptedException {
        synchronized (this) {
            while (!initDone) {
                if (initFailure != null) {
                    throw new CacheException(
                        "The cache for " + clientId + " is not initialized", initFailure);
                }
                wait();
            }
        }
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

        private final ReentrantLock offsetUpdateLock;
        private final Condition offsetReachedThreshold;
        private final Map<Integer, Long> lastReadOffsets = new ConcurrentHashMap<>();
        private int pollFailures = 0;

        public WorkerThread() {
            super("kafka-cache-reader-thread-" + topic);
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
                Map<TopicPartition, Long> checkpoints = checkpointHandler.checkpoints();
                for (final TopicPartition topicPartition : topicPartitions) {
                    final Long checkpoint = checkpoints.get(topicPartition);
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

        private int readToEndOffsets(Duration timeout) throws IOException {
            Set<TopicPartition> assignment = consumer.assignment();
            Map<TopicPartition, Long> endOffsets =
                retryOnWakeup(timeout, left -> consumer.endOffsets(assignment, left));
            log.info("Reading to end of offsets {}", endOffsets);

            int count = 0;
            // A topic deleted or truncated as it is read never reaches the end offsets taken, so
            // a read that has not moved for the timeout gives up, while one that moves goes on
            // however long it takes.
            long lastMoved = System.nanoTime();
            Map<TopicPartition, Long> positions =
                positions(endOffsets.keySet(), lastMoved + timeout.toNanos());
            // The positions last taken serve for both, as nothing moves them in between.
            while (!hasReadToEndOffsets(endOffsets, positions)) {
                // A poll swallows the wakeup from close(), so the read stops for it here instead.
                if (!isRunning()) {
                    throw new WakeupException();
                }
                // A poll waits no longer than the read has left before it gives up.
                long left = timeout.toNanos() - (System.nanoTime() - lastMoved);
                Duration pollFor = Duration.ofNanos(
                    Math.max(0, Math.min(TimeUnit.MILLISECONDS.toNanos(pollTimeout), left)));
                int polled = poll(pollFor, false);
                count += polled;
                if (polled > 0) {
                    // Records came, so the read moved: the lookups that follow get their time from
                    // now, rather than what was left before, perhaps nothing.
                    lastMoved = System.nanoTime();
                }
                Map<TopicPartition, Long> now =
                    positions(endOffsets.keySet(), lastMoved + timeout.toNanos());
                // A position goes back only when the consumer resets it, as the log has ended
                // below it: the topic was deleted and created again, or truncated, so the end
                // offsets taken no longer hold. Such a read is no progress, but a failure.
                for (Map.Entry<TopicPartition, Long> entry : now.entrySet()) {
                    Long before = positions.get(entry.getKey());
                    if (before != null && entry.getValue() < before) {
                        throw new IOException("Reading topic " + topic + " went back from offset "
                            + before + " to " + entry.getValue() + " on " + entry.getKey()
                            + ", short of end offsets " + endOffsets
                            + ": the topic was deleted or truncated as it was read");
                    }
                }
                boolean moved = hasMoved(positions, now);
                positions = now;
                if (moved) {
                    lastMoved = System.nanoTime();
                } else if (System.nanoTime() - lastMoved >= timeout.toNanos()) {
                    // A timeout, as a stalled broker may yet answer: a caller may try again.
                    throw new org.apache.kafka.common.errors.TimeoutException("Reading topic "
                        + topic + " made no progress for " + timeout.toMillis()
                        + " ms, short of end offsets " + endOffsets);
                }
            }
            log.info("During init or sync, processed {} records from topic {}", count, topic);
            return count;
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

        // The positions, each looked up with what is left before the deadline, so that together
        // they wait no longer than the read has left.
        private Map<TopicPartition, Long> positions(Collection<TopicPartition> topicPartitions,
                                                    long deadlineNanos) {
            Map<TopicPartition, Long> positions = new HashMap<>();
            for (TopicPartition tp : topicPartitions) {
                Duration left = Duration.ofNanos(Math.max(0, deadlineNanos - System.nanoTime()));
                positions.put(tp, position(tp, left));
            }
            return positions;
        }

        // Drops the partitions read to their end offsets, by the given positions of them all.
        private boolean hasReadToEndOffsets(Map<TopicPartition, Long> endOffsets,
                                            Map<TopicPartition, Long> positions) {
            endOffsets.entrySet().removeIf(entry -> positions.get(entry.getKey()) >= entry.getValue());
            return endOffsets.isEmpty();
        }

        private long position(TopicPartition tp, Duration timeout) {
            return retryOnWakeup(timeout, left -> consumer.position(tp, left));
        }

        /**
         * Calls a consumer method that blocks, and calls it again when a sync's wakeup cuts it
         * short, as a sync that arrives during a read is read for by the next read rather than
         * restart this one; the calls together wait no longer than the timeout, so that a stream
         * of syncs cannot keep a read that fails from failing. A wakeup from close() still stops
         * the read.
         */
        private <T> T retryOnWakeup(Duration timeout, Function<Duration, T> call) {
            long start = System.nanoTime();
            Duration left = timeout;
            while (true) {
                try {
                    return call.apply(left);
                } catch (WakeupException e) {
                    if (!isRunning()) {
                        throw e;
                    }
                    left = timeout.minusNanos(System.nanoTime() - start);
                    if (left.isNegative() || left.isZero()) {
                        // Not called again with no time left, as endOffsets() then returns none.
                        throw new org.apache.kafka.common.errors.TimeoutException("Reading topic "
                            + topic + " timed out after " + timeout.toMillis() + " ms");
                    }
                }
            }
        }

        @Override
        protected void doWork() {
            int numCallbacks;
            synchronized (KafkaCache.this) {
                numCallbacks = syncCallbacks.size();
            }

            int count = 0;
            if (numCallbacks > 0) {
                try {
                    count = readToEndOffsets(Duration.ofMillis(timeout));
                } catch (WakeupException e) {
                    // The cache is closing: the thread stops, and the syncs fail.
                    return;
                } catch (KafkaException | IOException e) {
                    log.error("Error while reading to end offsets", e);
                    // A retry could fail for good, as on a deleted topic: the syncs fail instead.
                    completeCallbacks(numCallbacks, 0, e);
                    return;
                }
            }

            completeCallbacks(numCallbacks, count, null);

            // A sync that came during the read, its wakeup perhaps spent there, is read for next,
            // rather than wait on a poll that its wakeup no longer cuts short.
            synchronized (KafkaCache.this) {
                if (!syncCallbacks.isEmpty()) {
                    return;
                }
            }
            // close() stops the thread before it wakes the consumer, and the read may have spent
            // that wakeup, so a poll now could wait for good.
            if (!isRunning()) {
                return;
            }
            poll();
        }

        @Override
        public void run() {
            Throwable failure = null;
            try {
                super.run();
            } catch (RuntimeException | Error e) {
                failure = e;
                throw e;
            } finally {
                stopSyncs(failure);
                // Here as well as in shutdown(), for a close() on this thread, which cannot wait
                // for it to stop.
                closeConsumer();
            }
        }

        /**
         * No read follows: the syncs waiting fail, and so do any that come later, with what the
         * thread died of, if it did. Called by the thread as it stops, and by close() once it
         * has, in either order, so a failure already known is kept.
         */
        private void stopSyncs(Throwable failure) {
            synchronized (KafkaCache.this) {
                readerStopped = true;
                if (readerFailure == null) {
                    readerFailure = failure;
                }
                CacheException error = readerStoppedException();
                for (CompletableFuture<Integer> cb; (cb = syncCallbacks.poll()) != null; ) {
                    cb.completeExceptionally(error);
                }
            }
            // Syncs waiting for their own writes, which no read will reach now, stop waiting.
            // Signalled outside the lock on the cache, which those syncs take inside this one.
            offsetUpdateLock.lock();
            try {
                offsetReachedThreshold.signalAll();
            } finally {
                offsetUpdateLock.unlock();
            }
        }

        private void completeCallbacks(int numCallbacks, int count, Throwable error) {
            synchronized (KafkaCache.this) {
                // Only invoke exactly the number of callbacks we found before the read to log end
                // since it is possible for another write + sync to sneak in the meantime
                for (int i = 0; i < numCallbacks; i++) {
                    CompletableFuture<Integer> cb = syncCallbacks.poll();
                    if (cb != null) {
                        if (error != null) {
                            cb.completeExceptionally(error);
                        } else {
                            cb.complete(count);
                        }
                    }
                }
            }
        }

        private int poll() {
            return poll(Duration.ofMillis(pollTimeout), true);
        }

        /**
         * Polls once. An error from the consumer is backed off from, as by the thread between
         * reads, or else thrown, as by a read: it fails with the error, as when its other calls
         * fail, rather than back off past its timeout and fail as having timed out.
         */
        private int poll(Duration timeout, boolean backOffOnError) {
            int count = 0;
            try {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
                pollFailures = 0;
                count = records.count();
                cacheUpdateHandler.startBatch(count);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    // Once the thread is stopping, as for a close() from an update handler on
                    // this thread, which closes the local cache and checkpoints as it returns, no
                    // more is applied, so none is checkpointed past what was stored.
                    if (!isRunning()) {
                        break;
                    }
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
                if (isRunning() && localCache.isPersistent()
                    && (checkpointBeforeInit || initialized.get())) {
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
                } else if (t instanceof KafkaException && !backOffOnError) {
                    throw t;
                } else if (t instanceof KafkaException) {
                    // e.g. AuthenticationException during a broker-side auth outage; back off before retrying
                    long backoffMs = Math.min(30_000L, 100L << Math.min(pollFailures++, 9));
                    log.error("Error while polling; retrying in {} ms", backoffMs, t);
                    try {
                        pause(backoffMs, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
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
            try {
                checkpointHandler.updateCheckpoints(newOffsets);
            } catch (final IOException e) {
                log.warn("Failed to write offset checkpoints", e);
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
                while (lastReadOffsets.get(partition) < offset && timeoutNs > 0 && !readerStopped) {
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
                // No reader will reach the offset: the write fails at once, as a sync does, with
                // what the reader died of, rather than time out as one a caller may try again.
                if (readerStopped) {
                    synchronized (KafkaCache.this) {
                        throw readerStoppedException();
                    }
                }
                throw new CacheTimeoutException(
                    "KafkaCacheTopic thread failed to reach target offset within the timeout interval. "
                        + "targetOffset: " + offset + ", offsetReached: " + lastReadOffsets.get(partition)
                        + ", timeout(ms): " + timeout.toMillis());
            }
        }

        private int waitUntilLastWrittenOffsets(Duration timeout) throws CacheException {
            Map<Integer, Long> lastOffsets = new HashMap<>(lastWrittenOffsets);
            if (!hasValidLastWrittenOffsets(lastOffsets)) {
                return -1;
            }
            int count = countUnreadOffsets(lastOffsets);
            if (count == 0) {
                return count;
            }
            try {
                offsetUpdateLock.lock();
                long timeoutNs = timeout.toNanos();
                while (countUnreadOffsets(lastOffsets) > 0 && timeoutNs > 0 && !readerStopped) {
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

            if (countUnreadOffsets(lastOffsets) == 0) {
                return count;
            } else if (!readerStopped) {
                log.warn("Could not read to last written offsets {}", lastOffsets);
            }
            // As when the reader thread has stopped: the sync then fails at once.
            return -1;
        }

        private boolean hasValidLastWrittenOffsets(Map<Integer, Long> lastOffsets) {
            return lastOffsets.keySet().containsAll(partitions);
        }

        private int countUnreadOffsets(Map<Integer, Long> lastOffsets) {
            return lastOffsets.entrySet().stream()
                .map(entry -> {
                    int lastWrittenPartition = entry.getKey();
                    long lastWrittenOffset = entry.getValue();
                    long lastReadOffset = lastReadOffsets.getOrDefault(lastWrittenPartition, -1L);
                    return Math.max((int)(lastWrittenOffset - lastReadOffset), 0);
                })
                .reduce(0, Integer::sum);
        }

        /**
         * Waits for the thread to stop even when interrupted, as a thread still running would go
         * on reading into the local cache and checkpointing past what it stored, once close()
         * has closed them. An interrupt is restored once the thread has stopped. On the thread
         * itself, as from an update handler, it only asks the thread to stop, which it does once
         * the handler returns.
         */
        @Override
        public void shutdown() {
            log.debug("Starting shutdown of KafkaTopicReader thread for {}.", clientId);

            super.initiateShutdown();
            if (consumer != null) {
                consumer.wakeup();
            }
            if (Thread.currentThread() == this) {
                return;
            }
            boolean interrupted = false;
            while (true) {
                try {
                    super.awaitShutdown();
                    // The thread counts its shutdown down before it stops the syncs, recording
                    // what it died of, and before it ends: joining it waits for both.
                    join();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            // For a thread that never started, which stops no syncs itself.
            stopSyncs(null);
            closeConsumer();
            if (interrupted) {
                Thread.currentThread().interrupt();
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
