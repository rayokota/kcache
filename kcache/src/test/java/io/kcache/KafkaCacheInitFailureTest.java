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

import io.kcache.exceptions.CacheInitializationException;
import io.kcache.utils.ClusterTestHarness;
import io.kcache.utils.StringUpdateHandler;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
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

    @Override
    protected void injectProperties(Properties props) {
        super.injectProperties(props);
        props.setProperty("delete.topic.enable", "true");
    }

    // A topic deleted as it is read never reaches the end offsets taken at the start: the read
    // gives up once it has not moved for the init timeout, rather than poll for good.
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
        }
        reader.close();
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
        return new KafkaCache<>(new KafkaCacheConfig(props), Serdes.String(), Serdes.String(),
            new StringUpdateHandler(), null);
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
