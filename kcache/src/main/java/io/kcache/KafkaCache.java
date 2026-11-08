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
import java.util.concurrent.atomic.AtomicReference;
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
    /**
     * Where the cache is in its life, from its making to its close. Not named State, which
     * WorkerThread, as a Thread, would take for Thread.State.
     */
    private enum Lifecycle {
        // Not initialized, or not since an init failed: init may be called.
        NEW,
        // An init is under way, before its reader thread starts: another init may not start,
        // and a close() stops it.
        INITIALIZING,
        // The reader thread has started, and init's cacheInitialized() handler runs: the cache
        // may be used, though init has not ended.
        INITIALIZED,
        // Init has ended, its handler having returned or thrown: syncs go on.
        READY,
        // close() has been called: no init may follow. Whether the rest of the cache is closed
        // yet is restClosed.
        CLOSED
    }

    // Changed only under this, and read without it as well.
    private volatile Lifecycle lifecycle = Lifecycle.NEW;
    // Changed only under this: why init has not made the cache ready, once it has failed or the
    // cache is closed first, so that the syncs waiting for it fail. So a CLOSED cache was ready
    // just when this is null. Read without the lock as well.
    private volatile Throwable initFailure;
    // Guarded by this: the thread running init, while one runs, which close() waits for.
    private Thread initThread;
    // Guarded by this: whether close() was called on the init thread, as from an update handler
    // during init, which cannot wait for init to end: init closes the cache once it ends.
    private boolean closeOnInitEnd;
    // Guarded by this: whether init is doing that close, as it ends. Its thread stays initThread
    // meanwhile, so that a close() waiting for init waits for that too, and finds what it failed
    // with.
    private boolean closingAfterInit;
    // Whether the rest of the cache is closed, by close() or by a reader thread closed from one
    // of its update handlers, so that it is closed once whichever does it first. Guarded by
    // restCloseLock.
    private boolean restClosed;
    // Held while the rest is closed, so that a second close() waits for the first to have closed
    // it, rather than return while it still closes. Not this, which syncs use meanwhile.
    private final Object restCloseLock = new Object();
    // What a close() left to another thread failed with, where nothing could throw it: on the
    // reader thread, closed from an update handler, or at the end of init, closed from a handler
    // during it. The application's own close() throws it instead. Guarded by itself.
    private final AtomicReference<Throwable> deferredCloseFailure = new AtomicReference<>();
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
    // Volatile, as close() reads it to stop an init under way on another thread.
    private volatile Consumer<byte[], byte[]> consumer;
    // Volatile, as close() reads it to stop the read of an init under way on another thread.
    private volatile WorkerThread kafkaTopicReader;
    private KafkaCacheConfig config;
    private final Map<Integer, Long> lastWrittenOffsets = new ConcurrentHashMap<>();
    private final Queue<CompletableFuture<Integer>> syncCallbacks = new ArrayDeque<>();
    // Guarded by this: once the reader thread has stopped, no sync is read for; and what it died
    // of, if it did, rather than stop as the cache closed. Written under the lock, but read
    // without it by syncs waiting for their own writes.
    private volatile boolean readerStopped;
    private Throwable readerFailure;
    // Guarded by this: whether the reader thread waits on its idle poll, which only a wakeup cuts
    // short for a sync. A reader that is reading reads for a sync next without one.
    private boolean readerIdle;

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
        synchronized (this) {
            switch (lifecycle) {
                case INITIALIZING:
                case INITIALIZED:
                    // Checked here, before either init makes anything, so that the one that
                    // fails leaves no clients or reader thread behind.
                    throw new CacheInitializationException("Illegal state while initializing cache for "
                        + clientId + ". Cache is already being initialized");
                case CLOSED:
                    if (initFailure != null) {
                        // Closed before it was ready. Its local cache and handlers are closed, or
                        // closing, and a close() after this init would not close the clients it
                        // made.
                        throw new ClosedDuringInitException(clientId, null);
                    }
                    // Closed once ready, as no failure says otherwise: as a ready cache.
                    // fall through
                case READY:
                    throw new CacheInitializationException("Illegal state while initializing cache for "
                        + clientId + ". Cache was already initialized");
                default:
                    break;
            }
            lifecycle = Lifecycle.INITIALIZING;
            initThread = Thread.currentThread();
            // A new init, as after one that failed: syncs wait for it, as for the first.
            initFailure = null;
        }
        Throwable failure = null;
        try {
            initCache();
        } catch (Throwable t) {
            // Recorded first, so that init ends as failed whatever discarding what it applied
            // does.
            failure = t;
            discardFailedInit(t);
            throw t;
        } finally {
            boolean closeNow;
            synchronized (this) {
                // All in one, so that no other init can start before this one has ended.
                if (failure != null) {
                    // Unless closed meanwhile, another init may be tried. A cache made ready,
                    // as before a cacheInitialized() handler that threw, stays so, and its syncs
                    // go on.
                    if (lifecycle == Lifecycle.INITIALIZING) {
                        lifecycle = Lifecycle.NEW;
                    }
                    if (lifecycle != Lifecycle.READY) {
                        initFailure = failure;
                    }
                }
                closeNow = closeOnInitEnd;
                closeOnInitEnd = false;
                if (closeNow) {
                    closingAfterInit = true;
                } else {
                    initThread = null;
                }
                // Syncs waiting for init go on, and a close() waiting for it to end, unless init
                // still closes the cache.
                notifyAll();
            }
            if (closeNow) {
                try {
                    closeAfterInit(failure);
                } finally {
                    synchronized (this) {
                        closingAfterInit = false;
                        initThread = null;
                        notifyAll();
                    }
                }
            }
        }
    }

    /**
     * The close() that an update handler called during init, on the init thread or the reader
     * thread, now that init has ended. What it fails with goes with init's own failure, if init
     * failed; otherwise the application's next close() throws it.
     */
    private void closeAfterInit(Throwable initFailed) {
        try {
            close();
        } catch (Throwable t) {
            if (initFailed != null) {
                initFailed.addSuppressed(t);
            } else {
                log.warn("Failed to close the Kafka cache for {} after init", clientId, t);
                // What another thread's close failed with, as the reader's, kept as it was, so
                // that the application's close() wraps it once.
                keepDeferredCloseFailure(
                    t instanceof DeferredCloseException ? t.getCause() : t);
            }
        }
    }

    /**
     * Keeps what a close() left to another thread failed with, for the application's close() to
     * throw, with any kept already: an Error first, which close() throws as is, with the other
     * suppressed.
     */
    private void keepDeferredCloseFailure(Throwable failure) {
        synchronized (deferredCloseFailure) {
            Throwable kept = deferredCloseFailure.get();
            if (kept == null) {
                deferredCloseFailure.set(failure);
            } else if (kept != failure) {
                boolean failureFirst = failure instanceof Error && !(kept instanceof Error);
                Throwable first = failureFirst ? failure : kept;
                first.addSuppressed(failureFirst ? kept : failure);
                deferredCloseFailure.set(first);
            }
        }
    }

    private Throwable takeDeferredCloseFailure() {
        synchronized (deferredCloseFailure) {
            return deferredCloseFailure.getAndSet(null);
        }
    }

    // Whether init has made the cache usable, though it may since have been closed.
    private boolean isInitialized() {
        Lifecycle current = lifecycle;
        return current == Lifecycle.INITIALIZED || current == Lifecycle.READY
            || (current == Lifecycle.CLOSED && initFailure == null);
    }

    private static boolean causedByWakeup(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof WakeupException) {
                return true;
            }
        }
        return false;
    }

    private void throwIfClosedDuringInit() {
        if (lifecycle == Lifecycle.CLOSED) {
            throw new ClosedDuringInitException(clientId, null);
        }
    }

    /** An init that fails as the cache was closed, before or while it ran. */
    private static class ClosedDuringInitException extends CacheInitializationException {
        ClosedDuringInitException(String clientId, Throwable cause) {
            super("Illegal state while initializing cache for " + clientId + ". Cache was closed",
                cause);
        }
    }

    /**
     * Drops what an init that failed applied to a local cache that is not persistent, so that an
     * init after it starts as a new cache would, rather than keep records that the topic, read
     * again, may no longer have. A persistent cache keeps them, as after a restart. So does one
     * the init got as far as initializing, as before a cacheInitialized() handler that threw,
     * whose records they then are.
     */
    private void discardFailedInit(Throwable failure) {
        if (localCache.isPersistent() || isInitialized()) {
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
            // An Error goes on in its place, rather than be hidden under a failure that may be
            // tried again; init has recorded its own failure first.
            failure.addSuppressed(e);
        }
    }

    private void initCache() {
        if (localCache.isPersistent()) {
            checkpointHandler.init();
            log.info("Successfully read checkpoints");
        }
        localCache.init();

        // A close() before the consumer is made has nothing to wake: init checks for it before
        // each step that would make or wait on something more.
        throwIfClosedDuringInit();
        if (!skipValidation) {
            createOrVerifyTopic();
            throwIfClosedDuringInit();
        }
        this.consumer = createConsumer(getConsumerProperties());
        int count;
        Map<TopicPartition, Long> checkpoints;
        try {
            throwIfClosedDuringInit();
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
            // A close() before the reader was made could not stop its read, so it is not begun.
            // One after it finds the reader, and stops the read, as both fields are volatile.
            throwIfClosedDuringInit();
            try {
                count = this.kafkaTopicReader.readToEndOffsets(Duration.ofMillis(initTimeout));
            } catch (IOException | KafkaException e) {
                // Whatever the read failed with, as an error from a poll, it fails init.
                throw new CacheInitializationException("Failed to read to end offsets", e);
            }
            checkpoints = checkpointHandler.checkpoints();
            synchronized (this) {
                // A close() once the read was done: the reader is not started.
                throwIfClosedDuringInit();
                // Started under the lock that close() marks the cache closed under, so that a
                // close() either finds it started or stops init before it starts; and before the
                // cache is marked initialized, so that a thread that fails to start fails init
                // as anything else here does.
                this.kafkaTopicReader.start();
                lifecycle = Lifecycle.INITIALIZED;
            }
        } catch (Throwable t) {
            // The clients are closed here, as the reader thread that would close the consumer
            // has not started. A close() waits for init to end, so it closes none of them. Any
            // failure, as an Error or a checked exception thrown sneakily from a handler, so that
            // a later init does not leak them.
            closeClients();
            if (lifecycle == Lifecycle.CLOSED && !(t instanceof ClosedDuringInitException)
                && causedByWakeup(t)) {
                // As a read that a close() stopped, rather than one that failed of itself as the
                // close came.
                throw new ClosedDuringInitException(clientId, t);
            }
            throw t;
        }
        try {
            cacheUpdateHandler.cacheInitialized(count, checkpoints);
        } finally {
            // The cache is ready, its reader running, even if the handler throws: syncs go on,
            // rather than fail for good on a cache that init cannot be tried again on. No close()
            // changes the lifecycle while the handler runs, as each waits for init or is left to
            // init's end.
            synchronized (this) {
                lifecycle = Lifecycle.READY;
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
        throwIfReaderStopped();

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
        boolean wake;
        synchronized (this) {
            throwIfReaderStopped();
            syncCallbacks.add(future);
            wake = readerIdle;
        }
        // Only an idle reader is woken: one that is reading would only have its read cut short,
        // and syncs that kept coming could keep it from ever reaching the end offsets.
        if (wake) {
            wakeConsumer();
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
            if (cause instanceof ReaderStoppedException) {
                // Failed by stopSyncs(), as the reader stopped: as a sync after it fails, with
                // an exception of its own, rather than the one all the syncs it failed share.
                throwIfReaderStopped();
            }
            throw new CacheException("Failed to read to end offsets", cause);
        }
    }

    // Guarded by this.
    // Fails once no reader will read, as the reader thread has died or the cache was closed,
    // with what the thread died of, if it did.
    private void throwIfReaderStopped() throws CacheException {
        if (readerStopped) {
            synchronized (this) {
                throw readerStoppedException();
            }
        }
    }

    private CacheException readerStoppedException() {
        return readerFailure != null
            ? new ReaderStoppedException(
                "The reader thread for " + clientId + " has died", readerFailure)
            : new ReaderStoppedException(
                "The reader thread for " + clientId + " has stopped, as the cache was closed", null);
    }

    /** A failure as the reader thread has stopped, by which a sync it failed is told apart. */
    private static class ReaderStoppedException extends CacheException {
        ReaderStoppedException(String message, Throwable cause) {
            super(message, cause);
        }
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

    // The consumer the cache reads its topic with; a test may read with one of its own.
    Consumer<byte[], byte[]> createConsumer(Properties props) {
        return new KafkaConsumer<>(props);
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
        // No reader will apply the write, which would then fail once written: it fails unwritten
        // instead, so that the caller knows the record was not written, as before trying it
        // again on another cache. A reader that stops just after this check still lets the
        // write through.
        throwIfReaderStopped();
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
        Consumer<byte[], byte[]> oldConsumer;
        Producer<byte[], byte[]> oldProducer;
        // Taken under this, so that a close() stopping init finds the clients all there or all
        // gone, but closed outside it, as closing the producer may wait on its sends.
        synchronized (this) {
            oldConsumer = consumer;
            oldProducer = producer;
            kafkaTopicReader = null;
            consumer = null;
            producer = null;
        }
        closeConsumer(oldConsumer);
        Utils.closeQuietly(oldProducer, "Kafka cache producer for " + clientId);
    }

    /**
     * Closes the consumer without waiting: it neither belongs to a group nor commits offsets, so
     * a timeout would only wait for its last fetch, which the broker holds on an idle topic for
     * up to fetch.max.wait.ms.
     */
    private void closeConsumer() {
        closeConsumer(consumer);
    }

    private void closeConsumer(Consumer<byte[], byte[]> toClose) {
        if (toClose != null) {
            Utils.closeQuietly(() -> toClose.close(Duration.ZERO),
                "Kafka cache consumer for " + clientId);
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            Thread current = Thread.currentThread();
            // An init whose reader has not started stops, on whichever thread close() is.
            if (lifecycle == Lifecycle.INITIALIZING) {
                markClosedBeforeReady();
                stopInit();
            }
            boolean onInitThread = current == initThread;
            boolean onReader = current == kafkaTopicReader;
            if (initThread != null && !closingAfterInit && (onInitThread || onReader)) {
                // From an update handler during init, on the init thread, or on the reader
                // thread while init's cacheInitialized() handler runs, neither of which can wait
                // for init to end: init closes the cache once it ends, rather than have it closed
                // under its handler. A reader thread stops now, leaving the rest to init.
                closeOnInitEnd = true;
                if (onReader) {
                    kafkaTopicReader.stopRead();
                }
                return;
            }
            // Waits for init to end, and for the close it does as it ends, so as to close nothing
            // init still uses. Not on the init thread, as that close itself, nor on the reader
            // thread, which that close waits for.
            if (!onInitThread && !onReader) {
                boolean interrupted = false;
                while (initThread != null) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            if (lifecycle != Lifecycle.READY && lifecycle != Lifecycle.CLOSED) {
                markClosedBeforeReady();
            }
            lifecycle = Lifecycle.CLOSED;
        }
        if (kafkaTopicReader != null) {
            if (Thread.currentThread() == kafkaTopicReader) {
                // On the reader thread, as from an update handler, which cannot wait for itself
                // to stop: it stops once the handler returns, and closes the rest itself then,
                // rather than have it closed under the record it is applying. So the rest is not
                // yet closed when this returns.
                kafkaTopicReader.stopAndClose();
                return;
            }
            kafkaTopicReader.shutdown();
        } else {
            closeConsumer();
        }
        closeRest();
        // Once the reader has stopped, as shutdown() waits for, it has closed the rest if a
        // handler asked it to.
        Throwable failure = takeDeferredCloseFailure();
        if (failure instanceof Error) {
            // Not wrapped in a checked exception, which a caller might catch and go on after.
            throw (Error) failure;
        } else if (failure != null) {
            throw new DeferredCloseException(clientId, failure);
        }
    }

    /** A close() that fails with what a close() left to another thread failed with. */
    private static class DeferredCloseException extends IOException {
        DeferredCloseException(String clientId, Throwable cause) {
            super("Failed to close the Kafka cache for " + clientId
                + " on the thread a close() was left to", cause);
        }
    }

    // Under this: closes the cache before init has made it ready, so that no init follows and
    // the syncs waiting for one fail.
    private void markClosedBeforeReady() {
        // The failure first, as a CLOSED cache with none was ready, to readers without the lock.
        initFailure = new CacheException(
            "The cache for " + clientId + " was closed before it was initialized");
        lifecycle = Lifecycle.CLOSED;
        notifyAll();
    }

    // Under this: stops an init under way, so that it fails soon. Its read stops, as does a
    // blocking call of its consumer before then, as making the reader finds the partitions; one
    // not yet begun is not begun, as init checks for a close once it has made the reader.
    private void stopInit() {
        WorkerThread reader = kafkaTopicReader;
        if (reader != null) {
            reader.stopRead();
        } else {
            wakeConsumer();
        }
    }

    // Cuts short a blocking call of the consumer, if there is one, or the next one made.
    private void wakeConsumer() {
        Consumer<byte[], byte[]> current = consumer;
        if (current != null) {
            current.wakeup();
        }
    }

    // Closes what the cache reads into and writes with, once no reader thread uses it. Each is
    // closed though one before it fails, as no later call closes any, and the first failure is
    // thrown, with the others logged.
    private void closeRest() throws IOException {
        synchronized (restCloseLock) {
            if (restClosed) {
                return;
            }
            restClosed = true;
            Utils.closeQuietly(producer, "Kafka cache producer for " + clientId);
            Closeables.closeAll("Kafka cache for " + clientId,
                localCache, checkpointHandler, cacheUpdateHandler);
        }
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
     *     initialized; a later init may still succeed after a failed one, though never after
     *     close
     */
    public void waitForInit() throws InterruptedException {
        synchronized (this) {
            while (lifecycle != Lifecycle.READY) {
                if (initFailure != null) {
                    throw new CacheException(
                        "The cache for " + clientId + " is not initialized", initFailure);
                }
                if (lifecycle == Lifecycle.CLOSED) {
                    // Closed once ready, as no failure says otherwise.
                    return;
                }
                wait();
            }
        }
    }

    private void assertInitialized() throws CacheException {
        if (!isInitialized()) {
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
        // Whether the thread closes the rest of the cache as it stops, for a close() on it.
        private volatile boolean closeOnStop;

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
            // The first lookups, as those after each poll, get a timeout of their own; the read's
            // starts once they are done.
            Map<TopicPartition, Long> positions =
                positions(endOffsets.keySet(), System.nanoTime() + timeout.toNanos());
            long lastMoved = System.nanoTime();
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
                count += poll(pollFor, false);
                // The lookups share a timeout of their own, from now, rather than what the poll
                // left of the read's, perhaps nothing. Only a partition still short of its end
                // offset moving keeps the read going: records the poll returned may be of one
                // already at its end, which goes on taking writes.
                Map<TopicPartition, Long> now =
                    positions(endOffsets.keySet(), System.nanoTime() + timeout.toNanos());
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
        // they wait no longer than until it.
        private Map<TopicPartition, Long> positions(Collection<TopicPartition> topicPartitions,
                                                    long deadlineNanos) {
            Map<TopicPartition, Long> positions = new HashMap<>();
            for (TopicPartition tp : topicPartitions) {
                positions.put(tp, position(tp, Duration.ofNanos(remainingNanos(deadlineNanos))));
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
         * short: one meant for the idle poll, from a sync that came just as that poll returned,
         * which then reaches the next read instead. The calls together wait no longer than the
         * timeout. A wakeup from close() still stops the read.
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

            if (!isRunning()) {
                // Asked to stop during the read, as by a close() from an update handler, which
                // closes the local cache once the thread stops: the syncs fail with the others,
                // rather than go on to a local cache being closed.
                return;
            }
            completeCallbacks(numCallbacks, count, null);

            // A sync that came during the read, which did not wake it, is read for next, rather
            // than wait on the idle poll. From here, under the same lock, a sync wakes that poll.
            synchronized (KafkaCache.this) {
                if (!syncCallbacks.isEmpty()) {
                    return;
                }
                readerIdle = true;
            }
            try {
                // close() stops the thread before it wakes the consumer, and the read may have
                // spent that wakeup, so a poll now could wait for good.
                if (!isRunning()) {
                    return;
                }
                poll();
            } finally {
                synchronized (KafkaCache.this) {
                    readerIdle = false;
                }
            }
        }

        @Override
        public void run() {
            Throwable failure = null;
            try {
                super.run();
            } catch (Throwable t) {
                // Any failure, as a checked exception thrown sneakily from a batch handler, so
                // that the syncs fail with what the thread died of, not as if the cache closed.
                failure = t;
                throw t;
            } finally {
                stopSyncs(failure);
                closeConsumer();
                if (closeOnStop) {
                    try {
                        closeRest();
                    } catch (Throwable t) {
                        // Already logged; kept rather than thrown, so that it neither replaces
                        // what the thread died of nor goes unreported.
                        keepDeferredCloseFailure(t);
                    }
                }
            }
        }

        /**
         * For a close() on this thread, as from an update handler: the thread stops once the
         * handler returns, after the batch it is applying, and then closes the rest of the cache
         * itself.
         */
        private void stopAndClose() {
            closeOnStop = true;
            super.initiateShutdown();
        }

        /**
         * Stops whatever read uses the consumer: this thread's own, once it has started, or
         * init's read to end offsets before then, on init's thread. It stops at its next check,
         * or at once from a blocking call. Unlike stopAndClose(), it leaves the rest of the cache
         * to whoever closes it.
         */
        private void stopRead() {
            super.initiateShutdown();
            wakeConsumer();
        }

        /**
         * No read follows: the syncs waiting fail, and so do any that come later, with what the
         * thread died of, if it did. Called by the thread as it stops, once.
         */
        private void stopSyncs(Throwable failure) {
            synchronized (KafkaCache.this) {
                readerStopped = true;
                readerFailure = failure;
                CacheException error = readerStoppedException();
                for (CompletableFuture<Integer> cb; (cb = syncCallbacks.poll()) != null; ) {
                    cb.completeExceptionally(error);
                }
            }
            // Syncs waiting for their own writes, which no read will reach now, stop waiting.
            // Signalled outside the lock on the cache, so that the two are never held together.
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
                if (localCache.isPersistent() && (checkpointBeforeInit || isInitialized())) {
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
                if (t instanceof WakeupException) {
                    // A wakeup cuts the consumer's poll short, before any batch began: none failed.
                    return count;
                }
                cacheUpdateHandler.failBatch(count, t);
                if (t instanceof KafkaException && !backOffOnError) {
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
                throwIfReaderStopped();
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
         * has closed them. An interrupt is restored once the thread has stopped. Not called on
         * the thread itself, which close() asks to stop with stopAndClose() instead.
         */
        @Override
        public void shutdown() {
            log.debug("Starting shutdown of KafkaTopicReader thread for {}.", clientId);

            stopRead();
            boolean interrupted = false;
            while (true) {
                try {
                    // Until the thread has ended, past stopping the syncs with what it died of.
                    join();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            // The thread stopped its syncs and closed its consumer as it ended. It has always
            // started: init starts it under the lock close() takes, close() waits for init, and an
            // init that fails before starting it drops it.
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
