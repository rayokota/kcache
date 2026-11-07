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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.kcache.exceptions.CacheInitializationException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

/** Whether a read to end offsets moved, as it decides when the read has stalled. */
public class KafkaCacheReadProgressTest {

    private static final TopicPartition P0 = new TopicPartition("topic", 0);
    private static final TopicPartition P1 = new TopicPartition("topic", 1);

    // A partition that has reached its end offset drops out of the later take: that is no move,
    // so that a read stalled beside it times out after one timeout rather than two.
    @Test
    public void testAPartitionDroppingOutIsNoMove() {
        assertFalse(KafkaCache.hasMoved(positions(5, 0), positions(5)));
    }

    @Test
    public void testAPositionThatGoesOnIsAMove() {
        assertTrue(KafkaCache.hasMoved(positions(5, 0), positions(6)));
        assertTrue(KafkaCache.hasMoved(positions(5, 0), positions(5, 1)));
    }

    @Test
    public void testPositionsThatStayAreNoMove() {
        assertFalse(KafkaCache.hasMoved(positions(5, 0), positions(5, 0)));
    }

    // A read stalled on one partition times out, though another, already at its end offset, goes
    // on taking writes that each poll returns: records of a partition the read no longer reads
    // for are no move of it.
    @Test
    public void testAReadStalledBesideAPartitionTakingWritesTimesOut() throws Exception {
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, "topic");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_PARTITIONS_CONFIG, "0,1");
        // No broker: no topic to verify, and no producer.
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_SKIP_VALIDATION_CONFIG, true);
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_READ_ONLY_CONFIG, true);
        props.put(KafkaCacheConfig.KAFKACACHE_INIT_TIMEOUT_CONFIG, 1000);
        Consumer<byte[], byte[]> consumer = new WritingConsumer();
        KafkaCache<String, String> cache = new KafkaCache<String, String>(
            new KafkaCacheConfig(props), Serdes.String(), Serdes.String(), null, null) {
            @Override
            Consumer<byte[], byte[]> createConsumer(Properties consumerProps) {
                return consumer;
            }
        };
        FutureTask<Void> init = new FutureTask<>(() -> {
            cache.init();
            return null;
        });
        Thread initializer = new Thread(init);
        initializer.setDaemon(true);
        initializer.start();
        try {
            init.get(30, TimeUnit.SECONDS);
            fail("init should fail");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof CacheInitializationException);
            assertTrue(e.getCause().getCause() instanceof RetriableException);
        } finally {
            cache.close();
        }
    }

    /**
     * Partition 0 has records to read but returns none; partition 1, already at its end offset,
     * takes a write that each poll returns.
     */
    private static class WritingConsumer extends MockConsumer<byte[], byte[]> {
        private long next;

        WritingConsumer() {
            super(OffsetResetStrategy.EARLIEST);
            updateBeginningOffsets(positions(0, 0));
            updateEndOffsets(positions(10, 0));
        }

        @Override
        public synchronized ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
            addRecord(new ConsumerRecord<>("topic", 1, next++,
                "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8)));
            return super.poll(timeout);
        }
    }

    // Positions of partitions 0, 1, and so on, in turn.
    private static Map<TopicPartition, Long> positions(long... offsets) {
        Map<TopicPartition, Long> positions = new HashMap<>();
        TopicPartition[] partitions = {P0, P1};
        for (int i = 0; i < offsets.length; i++) {
            positions.put(partitions[i], offsets[i]);
        }
        return positions;
    }
}
