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
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
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
    public void testSimpleGetAfterFailure() throws Exception {
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
    public void testDeleteAfterRestart() throws Exception {
        Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance();
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
            // recreate kafka store
            kafkaCache = createAndInitKafkaCacheInstance();
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

    protected Cache<String, String> createAndInitKafkaCacheInstance() throws Exception {
        Properties props = getKafkaCacheProperties();
        return CacheUtils.createAndInitKafkaCacheInstance(props);
    }

    protected Properties getKafkaCacheProperties() throws Exception {
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KafkaCacheConfig.KAFKACACHE_BACKING_CACHE_CONFIG, CacheType.MEMORY.name().toLowerCase());
        return props;
    }
}
