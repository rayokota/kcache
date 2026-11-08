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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.kcache.exceptions.CacheInitializationException;
import io.kcache.utils.ClusterTestHarness;
import io.kcache.utils.InMemoryCache;
import io.kcache.utils.StringUpdateHandler;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.ObjectName;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

public class KafkaCacheInitFailureTest extends ClusterTestHarness {

    // An init that fails once the reader thread exists: the topic has one partition, not three.
    @Test
    public void testCloseAfterAFailedInitReturns() throws Exception {
        Properties props = properties("failed-read");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_PARTITIONS_CONFIG, "0,1,2");
        KafkaCache<String, String> cache = cache(props);
        failInit(cache);
        FutureTask<Void> close = new FutureTask<>(() -> {
            cache.close();
            return null;
        });
        Thread closer = new Thread(close);
        closer.setDaemon(true);
        closer.start();
        close.get(30, TimeUnit.SECONDS);
    }

    // A cache closed before it was initialized cannot be initialized, as its local cache and
    // handlers are closed.
    @Test
    public void testAnInitAfterCloseFails() throws Exception {
        KafkaCache<String, String> cache = cache(properties("init-after-close"));
        cache.close();
        try {
            cache.init();
            fail("init should fail");
        } catch (CacheInitializationException e) {
            assertTrue(e.getMessage().contains("Cache was closed"));
        }
    }

    // An init while another is under way fails at once, before it makes any clients or reader
    // thread, and the one under way still succeeds.
    @Test
    public void testAnInitWhileAnotherIsUnderWayFails() throws Exception {
        BlockingInitCache localCache = new BlockingInitCache();
        KafkaCache<String, String> cache = new KafkaCache<>(
            new KafkaCacheConfig(properties("concurrent-init")), Serdes.String(), Serdes.String(),
            new StringUpdateHandler(), localCache);
        Started first = start(cache::init);
        try {
            assertTrue(localCache.entered.await(30, TimeUnit.SECONDS));
            try {
                cache.init();
                fail("init should fail");
            } catch (CacheInitializationException e) {
                assertTrue(e.getMessage().contains("already being initialized"));
            }
            localCache.release.countDown();
            first.get(30, TimeUnit.SECONDS);
            Thread reader = cache.getWorkerThread();
            assertTrue(reader.isAlive());
        } finally {
            localCache.release.countDown();
            cache.close();
        }
    }

    // A close() while init is under way, before the reader thread starts, fails that init, which
    // then starts no reader thread and closes the clients it made.
    @Test
    public void testACloseWhileInitIsUnderWayFailsIt() throws Exception {
        BlockingInitCache localCache = new BlockingInitCache();
        KafkaCache<String, String> cache = new KafkaCache<>(
            new KafkaCacheConfig(properties("close-during-init")), Serdes.String(), Serdes.String(),
            new StringUpdateHandler(), localCache);
        Started init = start(cache::init);
        Started close;
        try {
            assertTrue(localCache.entered.await(30, TimeUnit.SECONDS));
            close = start(cache::close);
            // It waits for init to end, rather than close the local cache init is using.
            awaitWaiting(close);
            assertEquals(0, localCache.closes.get());
        } finally {
            localCache.release.countDown();
        }
        close.get(30, TimeUnit.SECONDS);
        assertEquals(1, localCache.closes.get());
        assertClosedDuringInit(init);
        assertNull(cache.getWorkerThread());
    }

    // A close() during init's read stops the read, waits for init to end, and fails it, which
    // then starts no reader thread and closes the clients it made.
    @Test
    public void testACloseDuringInitsReadStopsTheRead() throws Exception {
        String topic = "close-during-read";
        Properties writing = properties("close-during-read-writer");
        writing.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        try (KafkaCache<String, String> writer = cache(writing)) {
            writer.init();
            for (int i = 0; i < 3; i++) {
                writer.put("k" + i, "v" + i);
            }
        }
        Properties props = properties("close-during-read");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        // Long enough that only a stopped read fails within the test's wait.
        props.put(KafkaCacheConfig.KAFKACACHE_INIT_TIMEOUT_CONFIG, 120000);
        // A record at a time, so that the read polls again after the first.
        props.put("kafkacache." + ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put("kafkacache." + ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1);
        HoldingFirstUpdate handler = new HoldingFirstUpdate();
        KafkaCache<String, String> reader = cache(props, handler);
        Started init = start(reader::init);
        Started close;
        try {
            assertTrue(handler.entered.await(30, TimeUnit.SECONDS));
            close = start(reader::close);
            awaitWaiting(close);
        } finally {
            handler.release.countDown();
        }
        close.get(30, TimeUnit.SECONDS);
        assertClosedDuringInit(init);
        // The read stopped after the record it was applying, short of the other two.
        assertEquals(1, handler.updates.get());
        assertNull(reader.getWorkerThread());
    }

    // A close() from an update handler on the init thread, during init's read, cannot wait for
    // init: it stops the read, and init closes the cache once it ends, and fails. The rest is
    // closed once, though the application closes the cache too.
    @Test
    public void testACloseFromAHandlerDuringInitsReadIsDoneAsInitEnds() throws Exception {
        String topic = "close-from-handler-during-read";
        Properties writing = properties("close-from-handler-during-read-writer");
        writing.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        try (KafkaCache<String, String> writer = cache(writing)) {
            writer.init();
            for (int i = 0; i < 3; i++) {
                writer.put("k" + i, "v" + i);
            }
        }
        Properties props = properties("close-from-handler-during-read");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        // A record at a time, so that the read polls again after the first.
        props.put("kafkacache." + ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put("kafkacache." + ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1);
        BlockingInitCache localCache = new BlockingInitCache();
        localCache.release.countDown();
        ClosingOnFirstUpdate handler = new ClosingOnFirstUpdate();
        KafkaCache<String, String> reader = new KafkaCache<>(new KafkaCacheConfig(props),
            Serdes.String(), Serdes.String(), handler, localCache);
        handler.cache = reader;
        try {
            reader.init();
            fail("init should fail");
        } catch (CacheInitializationException e) {
            assertTrue(e.getMessage().contains("Cache was closed"));
        }
        // The read stopped after the record the handler closed the cache from.
        assertEquals(1, handler.updates.get());
        assertEquals(1, localCache.closes.get());
        assertNull(reader.getWorkerThread());
        reader.close();
        assertEquals(1, localCache.closes.get());
    }

    /** Closes its cache from the first update it handles. */
    private static class ClosingOnFirstUpdate extends StringUpdateHandler {
        volatile KafkaCache<String, String> cache;
        final AtomicInteger updates = new AtomicInteger();

        @Override
        public void handleUpdate(String key, String value, String oldValue,
                                 TopicPartition tp, long offset, long timestamp) {
            super.handleUpdate(key, value, oldValue, tp, offset, timestamp);
            if (updates.incrementAndGet() == 1) {
                try {
                    cache.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private static void assertClosedDuringInit(FutureTask<Void> init) throws Exception {
        try {
            init.get(30, TimeUnit.SECONDS);
            fail("init should fail");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof CacheInitializationException);
            assertTrue(e.getCause().getMessage().contains("Cache was closed"));
        }
    }

    private interface Action {
        void run() throws Exception;
    }

    /** An action run on a thread of its own, which it keeps, to see whether it waits. */
    private static class Started extends FutureTask<Void> {
        final Thread thread;

        Started(Action action) {
            super(() -> {
                action.run();
                return null;
            });
            thread = new Thread(this);
            thread.setDaemon(true);
        }
    }

    private static Started start(Action action) {
        Started task = new Started(action);
        task.thread.start();
        return task;
    }

    // Until the task's thread waits in close() for init to end, rather than anywhere else.
    private static void awaitWaiting(Started task) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!waitsInClose(task.thread)) {
            assertFalse("close() returned or failed before init ended", task.isDone());
            assertTrue("close() did not wait for init", System.nanoTime() < deadline);
            Thread.sleep(10);
        }
    }

    private static boolean waitsInClose(Thread thread) {
        if (thread.getState() != Thread.State.WAITING) {
            return false;
        }
        StackTraceElement[] stack = thread.getStackTrace();
        for (int i = 0; i < stack.length - 1; i++) {
            if (stack[i].getClassName().equals(Object.class.getName())
                && stack[i].getMethodName().equals("wait")) {
                // The first frame of ours past the wait.
                for (int j = i + 1; j < stack.length; j++) {
                    if (!stack[j].getClassName().equals(Object.class.getName())) {
                        return stack[j].getClassName().equals(KafkaCache.class.getName())
                            && stack[j].getMethodName().equals("close");
                    }
                }
            }
        }
        return false;
    }

    /** Holds the first init of the local cache until released, and counts its closes. */
    private static class BlockingInitCache extends InMemoryCache<String, String> {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger closes = new AtomicInteger();

        @Override
        public void close() {
            closes.incrementAndGet();
        }

        @Override
        public void init() {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            super.init();
        }
    }

    // An init that times out as the reader thread is made, seeking back from the end offsets of
    // partitions the topic does not have, fails as an init failure, as one that times out in
    // the read does.
    @Test
    public void testAnInitThatTimesOutAsTheReaderIsMadeFailsAsAnInitFailure() throws Exception {
        Properties props = properties("timed-out-seek");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_PARTITIONS_CONFIG, "0,1,2");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_PARTITIONS_OFFSET_CONFIG, "-1");
        KafkaCache<String, String> cache = cache(props);
        try {
            cache.init();
            fail("init should fail");
        } catch (CacheInitializationException e) {
            assertTrue(e.getCause() instanceof RetriableException);
        } finally {
            cache.close();
        }
    }

    // An init that fails as the reader thread is made, since the topic does not exist.
    @Test
    public void testAFailedInitLeavesNoConsumer() throws Exception {
        Properties props = properties("failed-subscribe");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, "missing");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_SKIP_VALIDATION_CONFIG, true);
        props.put("kafkacache." + ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        KafkaCache<String, String> cache = cache(props);
        failInit(cache);
        assertTrue("The consumer is closed", ManagementFactory.getPlatformMBeanServer()
            .queryNames(new ObjectName("kafka.consumer:type=app-info,id=failed-subscribe"), null)
            .isEmpty());
        cache.close();
    }

    // Deleted topics stay deleted, rather than coming back empty on the reader's next fetch.
    @Override
    protected void injectProperties(Properties props) {
        super.injectProperties(props);
        props.setProperty("auto.create.topics.enable", "false");
        props.setProperty("delete.topic.enable", "true");
    }

    // A topic deleted as it is read never reaches the end offsets taken at the start: the read
    // gives up, as a timeout, once it has not moved for the init timeout, rather than poll for
    // good.
    @Test
    public void testAReadOfATopicDeletedAsItIsReadFails() throws Exception {
        String topic = "deleted-as-read";
        Properties writing = properties("deleted-as-read-writer");
        writing.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        try (KafkaCache<String, String> writer = cache(writing)) {
            writer.init();
            for (int i = 0; i < 20; i++) {
                writer.put("k" + i, "v" + i);
            }
        }
        Properties props = properties("deleted-as-read");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        // A record at a time, so that most are still to be fetched once the topic is gone.
        props.put("kafkacache." + ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put("kafkacache." + ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1);
        props.put("kafkacache." + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            HoldingFirstPoll.class.getName());
        KafkaCache<String, String> reader = cache(props);
        FutureTask<Void> init = new FutureTask<>(() -> {
            reader.init();
            return null;
        });
        Thread initializer = new Thread(init);
        initializer.setDaemon(true);
        initializer.start();
        try {
            assertTrue(HoldingFirstPoll.entered.await(30, TimeUnit.SECONDS));
            try (Admin admin = Admin.create(Collections.singletonMap(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
                admin.deleteTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS);
            }
        } finally {
            HoldingFirstPoll.release.countDown();
        }
        try {
            init.get(60, TimeUnit.SECONDS);
            fail("init should fail");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof CacheInitializationException);
            assertTrue(e.getCause().getCause() instanceof RetriableException);
        }
        reader.close();
    }

    // An init whose thread is interrupted as it reads fails with the consumer's error at once,
    // rather than back off from it, which the interrupt cuts short, and poll again and again
    // until the read times out.
    @Test
    public void testAnInitInterruptedAsItReadsFailsWithTheInterrupt() throws Exception {
        String topic = "interrupted-as-read";
        Properties writing = properties("interrupted-as-read-writer");
        writing.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        try (KafkaCache<String, String> writer = cache(writing)) {
            writer.init();
            for (int i = 0; i < 3; i++) {
                writer.put("k" + i, "v" + i);
            }
        }
        Properties props = properties("interrupted-as-read");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        // A record at a time, so that the read polls again after the first.
        props.put("kafkacache." + ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put("kafkacache." + ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1);
        HoldingFirstUpdate handler = new HoldingFirstUpdate();
        KafkaCache<String, String> reader = cache(props, handler);
        FutureTask<Void> init = new FutureTask<>(() -> {
            reader.init();
            return null;
        });
        Thread initializer = new Thread(init);
        initializer.setDaemon(true);
        initializer.start();
        try {
            assertTrue(handler.entered.await(30, TimeUnit.SECONDS));
            initializer.interrupt();
        } finally {
            handler.release.countDown();
        }
        try {
            init.get(60, TimeUnit.SECONDS);
            fail("init should fail");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof CacheInitializationException);
            assertTrue(e.getCause().getCause() instanceof InterruptException);
        } finally {
            reader.close();
        }
    }

    // An init that fails as it reads keeps none of what it read, so that an init after it, of
    // the topic created again empty, holds nothing, as a new cache would.
    @Test
    public void testAnInitAfterOneThatFailedAsItReadKeepsNothingOfIt() throws Exception {
        String topic = "init-again";
        Properties writing = properties("init-again-writer");
        writing.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        try (KafkaCache<String, String> writer = cache(writing)) {
            writer.init();
            for (int i = 0; i < 20; i++) {
                writer.put("k" + i, "v" + i);
            }
        }
        Properties props = properties("init-again");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        // A record at a time, so that most are still to be fetched once the topic is gone.
        props.put("kafkacache." + ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put("kafkacache." + ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1);
        HoldingFirstUpdate handler = new HoldingFirstUpdate();
        KafkaCache<String, String> reader = cache(props, handler);
        FutureTask<Void> init = new FutureTask<>(() -> {
            reader.init();
            return null;
        });
        Thread initializer = new Thread(init);
        initializer.setDaemon(true);
        initializer.start();
        try (Admin admin = Admin.create(Collections.singletonMap(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            // The read holds record 0, already applied to the local cache, as the topic goes.
            try {
                assertTrue(handler.entered.await(30, TimeUnit.SECONDS));
                admin.deleteTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS);
            } finally {
                handler.release.countDown();
            }
            try {
                init.get(60, TimeUnit.SECONDS);
                fail("init should fail");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof CacheInitializationException);
            }
            awaitDeleted(admin, topic);
        }
        try {
            // Init creates the topic again, empty.
            reader.init();
            assertNull(reader.get("k0"));
            assertEquals(0, reader.size());
        } finally {
            reader.close();
        }
    }

    // Waits, for at most 30 seconds, until the broker has finished deleting the topic.
    private static void awaitDeleted(Admin admin, String topic) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (admin.listTopics().names().get(30, TimeUnit.SECONDS).contains(topic)) {
            assertTrue(System.nanoTime() < deadline);
            Thread.sleep(100);
        }
    }

    /** Holds the first update, on whichever thread applies it, until released. */
    private static class HoldingFirstUpdate extends StringUpdateHandler {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger updates = new AtomicInteger();

        @Override
        public void handleUpdate(String key, String value, String oldValue,
                                 TopicPartition tp, long offset, long timestamp) {
            updates.incrementAndGet();
            super.handleUpdate(key, value, oldValue, tp, offset, timestamp);
            if (entered.getCount() > 0) {
                entered.countDown();
                try {
                    release.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /** Holds the first poll that returns records, until released. */
    public static class HoldingFirstPoll implements ConsumerInterceptor<byte[], byte[]>,
        ProducerInterceptor<byte[], byte[]> {
        static final CountDownLatch entered = new CountDownLatch(1);
        static final CountDownLatch release = new CountDownLatch(1);

        @Override
        public ConsumerRecords<byte[], byte[]> onConsume(ConsumerRecords<byte[], byte[]> records) {
            if (!records.isEmpty() && entered.getCount() > 0) {
                entered.countDown();
                try {
                    release.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return records;
        }

        @Override
        public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        }

        @Override
        public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }

    private Properties properties(String clientId) {
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KafkaCacheConfig.KAFKACACHE_BACKING_CACHE_CONFIG, CacheType.MEMORY.toString());
        props.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, clientId);
        props.put(KafkaCacheConfig.KAFKACACHE_INIT_TIMEOUT_CONFIG, 2000);
        return props;
    }

    private static KafkaCache<String, String> cache(Properties props) {
        return cache(props, new StringUpdateHandler());
    }

    private static KafkaCache<String, String> cache(Properties props, StringUpdateHandler handler) {
        return new KafkaCache<>(new KafkaCacheConfig(props), Serdes.String(), Serdes.String(),
            handler, null);
    }

    private static void failInit(KafkaCache<String, String> cache) {
        try {
            cache.init();
            fail("init should fail");
        } catch (RuntimeException expected) {
            // a CacheInitializationException, or the consumer's own failure
        }
    }
}
