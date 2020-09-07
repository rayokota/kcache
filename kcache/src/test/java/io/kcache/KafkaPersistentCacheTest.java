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

import static org.junit.Assert.assertEquals;

import io.kcache.exceptions.CacheException;
import io.kcache.utils.Caches;
import io.kcache.utils.OffsetCheckpoint;
import io.kcache.utils.StringUpdateHandler;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KafkaPersistentCacheTest extends KafkaCacheTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaPersistentCacheTest.class);

    @Rule
    public final TemporaryFolder dir = new TemporaryFolder();

    protected final String topic = KafkaCacheConfig.DEFAULT_KAFKACACHE_TOPIC;

    protected abstract Cache<String, String> getCache();

    @After
    @Override
    public void teardown() throws IOException {
        try (OffsetCheckpoint offsetCheckpoint = new OffsetCheckpoint(dir.getRoot().toString(), 0, topic)) {
            offsetCheckpoint.delete();
        }
        super.teardown();
    }

    @Override
    protected Cache<String, String> createAndInitKafkaCacheInstance(String bootstrapServers) {
        Cache<String, String> rocksDBCache = getCache();
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KafkaCacheConfig.KAFKACACHE_CHECKPOINT_DIR_CONFIG, dir.getRoot().toString());
        KafkaCacheConfig config = new KafkaCacheConfig(props);
        Cache<String, String> kafkaCache = Caches.concurrentCache(
            new KafkaCache<>(config,
                Serdes.String(),
                Serdes.String(),
                new StringUpdateHandler(),
                rocksDBCache));
        kafkaCache.init();
        return kafkaCache;
    }

    @Test
    public void testCheckpointBeforeAndAfterRestart() throws Exception {
        Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance(bootstrapServers);
        String key = "Kafka";
        String value = "Rocks";
        String key2 = "Hello";
        String value2 = "World";
        try {
            try {
                kafkaCache.put(key, value);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store put(Kafka, Rocks) operation failed", e);
            }
            String retrievedValue;
            try {
                retrievedValue = kafkaCache.get(key);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertEquals("Retrieved value should match entered value", value, retrievedValue);
            kafkaCache.close();

            final Map<TopicPartition, Long> offsets = Collections.singletonMap(new TopicPartition(topic, 0), 1L);
            final Map<TopicPartition, Long> result = readOffsetsCheckpoint();
            assertEquals(result, offsets);

            // recreate kafka store
            kafkaCache = createAndInitKafkaCacheInstance(bootstrapServers);
            try {
                kafkaCache.put(key2, value2);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store put(Hello, World) operation failed", e);
            }
        } finally {
            kafkaCache.close();
        }

        final Map<TopicPartition, Long> offsets = Collections.singletonMap(new TopicPartition(topic, 0), 2L);
        final Map<TopicPartition, Long> result = readOffsetsCheckpoint();
        assertEquals(result, offsets);
    }

    private Map<TopicPartition, Long> readOffsetsCheckpoint() throws IOException {
        try (OffsetCheckpoint offsetCheckpoint = new OffsetCheckpoint(dir.getRoot().toString(), 0, topic)) {
            return offsetCheckpoint.read();
        }
    }
}
