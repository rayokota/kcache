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
import io.kcache.utils.InMemoryCache;
import io.kcache.utils.StringUpdateHandler;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KafkaCacheTest extends ClusterTestHarness {

    private static final Logger log = LoggerFactory.getLogger(KafkaCacheTest.class);

    @Before
    public void setup() {
        log.debug("bootstrapservers = " + bootstrapServers);
    }

    @After
    public void teardown() {
        log.debug("Shutting down");
    }

    @Test
    public void testInitialization() throws IOException {
        Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance(bootstrapServers);
        kafkaCache.close();
    }

    @Test(expected = CacheInitializationException.class)
    public void testDoubleInitialization() throws Exception {
        try (Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance(bootstrapServers)) {
            kafkaCache.init();
        }
    }

    @Test
    public void testSimplePut() throws Exception {
        try (Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance(bootstrapServers)) {
            String key = "Kafka";
            String value = "Rocks";
            kafkaCache.put(key, value);
            String retrievedValue = kafkaCache.get(key);
            assertEquals("Retrieved value should match entered value", value, retrievedValue);
        }
    }

    @Test
    public void testSimpleGetAfterFailure() throws Exception {
        Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance(bootstrapServers);
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

        // recreate kafka store
        kafkaCache = createAndInitKafkaCacheInstance(bootstrapServers);
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
        try (Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance(bootstrapServers)) {
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
        Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance(bootstrapServers);
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
            kafkaCache = createAndInitKafkaCacheInstance(bootstrapServers);
            // verify that key still doesn't exist in the store
            retrievedValue = value;
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

    protected Cache<String, String> createAndInitKafkaCacheInstance(String bootstrapServers) {
        Cache<String, String> inMemoryCache = new InMemoryCache<>();
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        KafkaCacheConfig config = new KafkaCacheConfig(props);
        Cache<String, String> kafkaCache =
            new KafkaCache<>(config,
                Serdes.String(),
                Serdes.String(),
                new StringUpdateHandler(),
                inMemoryCache);
        kafkaCache.init();
        return kafkaCache;
    }
}
