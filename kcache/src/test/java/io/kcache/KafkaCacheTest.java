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
import io.kcache.utils.ClusterTestHarness;
import io.kcache.utils.CustomPartitioner;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class KafkaCacheTest extends ClusterTestHarness {

    private static final Logger log = LoggerFactory.getLogger(KafkaCacheTest.class);

    protected final String topic = KafkaCacheConfig.DEFAULT_KAFKACACHE_TOPIC;

    @Before
    public void setup() {
        log.debug("bootstrapservers = {}", bootstrapServers);
    }

    @After
    public void teardown() throws IOException {
        log.debug("Shutting down");
    }

    @Test
    public void testInitialization() throws Exception {
        Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance();
        kafkaCache.close();
    }

    @Test(expected = CacheInitializationException.class)
    public void testDoubleInitialization() throws Exception {
        try (Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance()) {
            kafkaCache.init();
        }
    }

    @Test
    public void testSimplePut() throws Exception {
        try (Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance()) {
            String key = "Kafka";
            String value = "Rocks";
            kafkaCache.put(key, value);
            String retrievedValue = kafkaCache.get(key);
            assertEquals("Retrieved value should match entered value", value, retrievedValue);
        }
    }

    @Test
    public void testSimplePutAll() throws Exception {
        try (Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance()) {
            Map<String, String> entries = new HashMap<>();
            String key = "Kafka";
            String value = "Rocks";
            entries.put(key, value);
            String key2 = "Kafka2";
            String value2 = "Rocks2";
            entries.put(key2, value2);
            kafkaCache.putAll(entries);
            String retrievedValue = kafkaCache.get(key);
            assertEquals("Retrieved value should match entered value", value, retrievedValue);
            retrievedValue = kafkaCache.get(key2);
            assertEquals("Retrieved value should match entered value", value2, retrievedValue);
        }
    }

    @Test
    public void testSimpleGetAfterRestart() throws Exception {
        Properties props = getKafkaCacheProperties();
        Cache<String, String> kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(props);
        String key = "Kafka";
        String value = "Rocks";
        String retrievedValue;
        try {
            try {
                kafkaCache.put(key, value);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store put(Kafka, Rocks) operation failed", e);
            }
            try {
                retrievedValue = kafkaCache.get(key);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertEquals("Retrieved value should match entered value", value, retrievedValue);
        } finally {
            kafkaCache.close();
        }

        // recreate kafka store with same props
        kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(props);
        try {
            try {
                retrievedValue = kafkaCache.get(key);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertEquals("Retrieved value should match entered value", value, retrievedValue);
        } finally {
            kafkaCache.close();
        }
    }

    @Test
    public void testSimpleDelete() throws Exception {
        try (Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance()) {
            String key = "Kafka";
            String value = "Rocks";
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
            try {
                kafkaCache.remove(key);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store delete(Kafka) operation failed", e);
            }
            // verify that value is deleted
            try {
                retrievedValue = kafkaCache.get(key);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertNull("Value should have been deleted", retrievedValue);
        }
    }

    @Test
    public void testSimpleDeleteAfterRestart() throws Exception {
        Properties props = getKafkaCacheProperties();
        Cache<String, String> kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(props);
        String key = "Kafka";
        String value = "Rocks";
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
            // delete the key
            try {
                kafkaCache.remove(key);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store delete(Kafka) operation failed", e);
            }
            // verify that key is deleted
            try {
                retrievedValue = kafkaCache.get(key);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertNull("Value should have been deleted", retrievedValue);
            kafkaCache.close();
            // recreate kafka store with same props
            kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(props);
            // verify that key still doesn't exist in the store
            try {
                retrievedValue = kafkaCache.get(key);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertNull("Value should have been deleted", retrievedValue);
        } finally {
            kafkaCache.close();
        }
    }

    @Test
    public void testTopicAdditionalConfigs() throws Exception {
        Properties kafkaCacheProps = getKafkaCacheProperties();
        kafkaCacheProps.put("kafkacache.topic.config.delete.retention.ms", "10000");
        kafkaCacheProps.put("kafkacache.topic.config.segment.ms", "10000");
        try (Cache<String, String> kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(kafkaCacheProps)) {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            ConfigResource configResource = new ConfigResource(
                ConfigResource.Type.TOPIC,
                KafkaCacheConfig.DEFAULT_KAFKACACHE_TOPIC
            );
            Map<ConfigResource, Config> topicConfigs;
            try (AdminClient admin = AdminClient.create(props)) {
                topicConfigs = admin.describeConfigs(Collections.singleton(configResource))
                    .all().get(60, TimeUnit.SECONDS);
            }

            Config config = topicConfigs.get(configResource);
            assertNotNull(config.get("delete.retention.ms"));
            assertEquals("10000", config.get("delete.retention.ms").value());
            assertNotNull(config.get("segment.ms"));
            assertEquals("10000", config.get("segment.ms").value());
        }
    }

    @Test
    public void testSyncAfterPut() throws Exception {
        try (Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance()) {
            String key = "Kafka";
            String value = "Rocks";
            try {
                kafkaCache.put(key, value);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store put(Kafka, Rocks) operation failed", e);
            }
            kafkaCache.sync();
            String retrievedValue;
            try {
                retrievedValue = kafkaCache.get(key);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertEquals("Retrieved value should match entered value", value, retrievedValue);
        }
    }

    @Test
    public void testSyncAfterRestart() throws Exception {
        Properties props = getKafkaCacheProperties();
        Cache<String, String> kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(props);
        String key = "Kafka";
        String value = "Rocks";
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
            // recreate kafka store
            kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(props);
            kafkaCache.sync();
            try {
                retrievedValue = kafkaCache.get(key);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertEquals("Retrieved value should match entered value", value, retrievedValue);
        } finally {
            kafkaCache.close();
        }
    }

    @Test
    public void testCustomPartitioner() throws Exception {
        Properties props = getKafkaCacheProperties();
        props.put("kafkacache." + ProducerConfig.PARTITIONER_CLASS_CONFIG,
            CustomPartitioner.class.getName());
        try (Cache<String, String> kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(props)) {
            String key = "Kafka";
            String value = "Rocks";
            String retrievedValue;
            try {
                kafkaCache.put(key, value);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store put(Kafka, Rocks) operation failed", e);
            }
            try {
                retrievedValue = kafkaCache.get(key);
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertEquals("Retrieved value should match entered value", value, retrievedValue);
        }

        assertEquals(Collections.singleton("Kafka"), CustomPartitioner.keys);
        assertEquals(Collections.singleton("Rocks"), CustomPartitioner.values);
    }

    @Test
    public void testValidateAndUpdateTopicMaxMessageSize() throws Exception {
        // Step 1: Set up KafkaCache config with 4MB
        Properties kafkaCacheProps = getKafkaCacheProperties();
        kafkaCacheProps.put(KafkaCacheConfig.KAFKACACHE_TOPIC_MAX_MESSAGE_BYTES_CONFIG, String.valueOf(4 * 1048576)); // 4MB
        KafkaCache<String, String> kafkaCache = new KafkaCache<>(new KafkaCacheConfig(kafkaCacheProps), null, null);
        kafkaCache.init();

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, KafkaCacheConfig.DEFAULT_KAFKACACHE_TOPIC);

            // Step 2: Manually set the topic's max.message.bytes to 1MB
            admin.alterConfigs(Collections.singletonMap(
                topicResource,
                new org.apache.kafka.clients.admin.Config(Collections.singleton(
                    new org.apache.kafka.clients.admin.ConfigEntry("max.message.bytes", "1048576")
                ))
            )).all().get(60, TimeUnit.SECONDS);

            // Verify initial value is 1MB
            Config configBefore = admin.describeConfigs(Collections.singleton(topicResource))
                .all().get(60, TimeUnit.SECONDS).get(topicResource);
            assertEquals("1048576", configBefore.get("max.message.bytes").value());

            // Step 3: Call validateAndUpdateTopicMaxMessageSize on the instance
            kafkaCache.validateAndUpdateTopicMaxMessageSize(admin);

            // Step 4: Verify the topic config is updated to 4MB
            Config configAfter = admin.describeConfigs(Collections.singleton(topicResource))
                .all().get(60, TimeUnit.SECONDS).get(topicResource);
            assertEquals(String.valueOf(4 * 1048576), configAfter.get("max.message.bytes").value());
        } finally {
            kafkaCache.close();
        }
    }

    protected Cache<String, String> createAndInitKafkaCacheInstance() throws Exception {
        Properties props = getKafkaCacheProperties();
        return CacheUtils.createAndInitKafkaCacheInstance(props);
    }

    protected Properties getKafkaCacheProperties() throws Exception {
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KafkaCacheConfig.KAFKACACHE_BACKING_CACHE_CONFIG, CacheType.MEMORY.toString());
        return props;
    }
}
