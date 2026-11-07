/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kcache;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.kcache.exceptions.CacheException;
import io.kcache.utils.ClusterTestHarness;
import io.kcache.utils.StringUpdateHandler;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

public class KafkaCacheSyncFailureTest extends ClusterTestHarness {

    // Deleted topics stay deleted, rather than coming back empty on the reader's next fetch.
    @Override
    protected void injectProperties(Properties props) {
        super.injectProperties(props);
        props.setProperty("auto.create.topics.enable", "false");
        props.setProperty("delete.topic.enable", "true");
    }

    // A sync on a deleted topic cannot read to its end: it fails, rather than retry for good.
    @Test
    public void testASyncOfADeletedTopicFails() throws Exception {
        String topic = "deleted-before-sync";
        Properties writing = properties("deleted-before-sync-writer", topic);
        try (KafkaCache<String, String> writer = cache(writing)) {
            writer.init();
            writer.put("k", "v");
        }
        // A reader that wrote nothing syncs by reading to the topic's end offsets.
        KafkaCache<String, String> reader = cache(properties("deleted-before-sync", topic));
        reader.init();
        try (Admin admin = Admin.create(Collections.singletonMap(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            admin.deleteTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS);
        }
        FutureTask<Void> sync = new FutureTask<>(() -> {
            reader.sync();
            return null;
        });
        Thread syncer = new Thread(sync);
        syncer.setDaemon(true);
        syncer.start();
        try {
            sync.get(60, TimeUnit.SECONDS);
            fail("sync should fail");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof CacheException);
        }
        reader.close();
    }

    // A sync of a closed cache has no reader thread to read for it: it fails at once.
    @Test
    public void testASyncAfterCloseFails() throws Exception {
        KafkaCache<String, String> cache = cache(properties("sync-after-close", "sync-after-close"));
        cache.init();
        cache.close();
        FutureTask<Void> sync = start(cache::sync);
        try {
            sync.get(30, TimeUnit.SECONDS);
            fail("sync should fail");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof CacheException);
        }
    }

    // A sync still waiting as the reader thread stops fails, rather than wait for good.
    @Test
    public void testASyncWaitingAsTheCacheClosesFails() throws Exception {
        String topic = "sync-as-closed";
        HoldingHandler handler = new HoldingHandler(0);
        try (KafkaCache<String, String> writer = cache(properties("sync-as-closed-writer", topic))) {
            writer.init();
            KafkaCache<String, String> reader = cache(properties("sync-as-closed", topic), handler);
            reader.init();
            writer.put("k", "v");
            handler.awaitEntered(0);

            FutureTask<Void> sync = start(reader::sync);
            awaitWaitingIn(thread(sync), "waitUntilEndOffsets");
            FutureTask<Void> close = start(() -> {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            awaitWaitingIn(thread(close), "awaitShutdown");
            handler.release(0);

            close.get(30, TimeUnit.SECONDS);
            try {
                sync.get(30, TimeUnit.SECONDS);
                fail("sync should fail");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof CacheException);
            }
        } finally {
            handler.releaseAll();
        }
    }

    // A sync whose read stops short of the end offsets it took, its topic deleted, fails once
    // the read has not moved for the timeout, as a timeout a caller may try again after.
    @Test
    public void testASyncWhoseReadStopsMovingFails() throws Exception {
        String topic = "sync-stops-moving";
        HoldingHandler handler = new HoldingHandler(0, 1);
        Properties props = properties("sync-stops-moving", topic);
        // A record at a time, so that most are still to be fetched once the topic is gone.
        props.put("kafkacache." + ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put("kafkacache." + ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1);
        KafkaCache<String, String> reader = cache(props, handler);
        try {
            try (KafkaCache<String, String> writer =
                     cache(properties("sync-stops-moving-writer", topic))) {
                writer.init();
                reader.init();
                for (int i = 0; i < 20; i++) {
                    writer.put("k" + i, "v" + i);
                }
            }
            // The reader thread holds record 0 as it polls; the sync's read then holds record 1,
            // after taking the end offsets.
            handler.awaitEntered(0);
            FutureTask<Void> sync = start(reader::sync);
            awaitWaitingIn(thread(sync), "waitUntilEndOffsets");
            handler.release(0);
            handler.awaitEntered(1);
            try (Admin admin = Admin.create(Collections.singletonMap(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
                admin.deleteTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS);
            }
            handler.release(1);

            try {
                sync.get(60, TimeUnit.SECONDS);
                fail("sync should fail");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof CacheException);
                assertTrue(e.getCause().getCause() instanceof RetriableException);
            }
        } finally {
            handler.releaseAll();
            reader.close();
        }
    }

    /** Holds the reader thread on the records at the given offsets, until each is released. */
    private static class HoldingHandler extends StringUpdateHandler {
        private final Map<Long, CountDownLatch> entered = new ConcurrentHashMap<>();
        private final Map<Long, CountDownLatch> released = new ConcurrentHashMap<>();

        HoldingHandler(long... offsets) {
            for (long offset : offsets) {
                entered.put(offset, new CountDownLatch(1));
                released.put(offset, new CountDownLatch(1));
            }
        }

        @Override
        public void handleUpdate(String key, String value, String oldValue,
                                 TopicPartition tp, long offset, long timestamp) {
            super.handleUpdate(key, value, oldValue, tp, offset, timestamp);
            CountDownLatch release = released.get(offset);
            if (release != null
                && Thread.currentThread().getName().startsWith("kafka-cache-reader-thread")) {
                entered.get(offset).countDown();
                try {
                    release.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        void awaitEntered(long offset) throws InterruptedException {
            assertTrue(entered.get(offset).await(30, TimeUnit.SECONDS));
        }

        void release(long offset) {
            released.get(offset).countDown();
        }

        void releaseAll() {
            released.values().forEach(CountDownLatch::countDown);
        }
    }

    private final Map<FutureTask<Void>, Thread> threads = new ConcurrentHashMap<>();

    private FutureTask<Void> start(Runnable task) {
        FutureTask<Void> future = new FutureTask<>(task, null);
        Thread thread = new Thread(future);
        thread.setDaemon(true);
        threads.put(future, thread);
        thread.start();
        return future;
    }

    private Thread thread(FutureTask<Void> future) {
        return threads.get(future);
    }

    // Spins, for at most 30 seconds, until the thread waits inside the method.
    private static void awaitWaitingIn(Thread thread, String method) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (thread.getState() != Thread.State.WAITING
            || Arrays.stream(thread.getStackTrace()).noneMatch(e -> e.getMethodName().equals(method))) {
            assertTrue(thread.isAlive() && System.nanoTime() < deadline);
            Thread.onSpinWait();
        }
    }

    private Properties properties(String clientId, String topic) {
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, clientId);
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        props.put(KafkaCacheConfig.KAFKACACHE_INIT_TIMEOUT_CONFIG, 2000);
        props.put(KafkaCacheConfig.KAFKACACHE_TIMEOUT_CONFIG, 2000);
        return props;
    }

    private static KafkaCache<String, String> cache(Properties props) {
        return cache(props, new StringUpdateHandler());
    }

    private static KafkaCache<String, String> cache(Properties props, StringUpdateHandler handler) {
        return new KafkaCache<>(new KafkaCacheConfig(props), Serdes.String(), Serdes.String(),
            handler, null);
    }
}
