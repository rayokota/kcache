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
import io.kcache.utils.Caches;
import io.kcache.utils.ClusterTestHarness;
import io.kcache.utils.StringUpdateHandler;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.kcache.KafkaCacheConfig.DEFAULT_KAFKACACHE_TOPIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class KafkaReadOnlyCacheTest extends ClusterTestHarness {

    private static final Logger log = LoggerFactory.getLogger(KafkaReadOnlyCacheTest.class);

    @Before
    public void setup() {
        log.debug("bootstrapservers = {}", bootstrapServers);
    }

    @After
    public void teardown() {
        log.debug("Shutting down");
    }

    @Test(expected = CacheInitializationException.class)
    public void testInitialization() throws IOException {
        try (Cache<String, String> cache = createKafkaCacheInstance()) {
            cache.init();
        }
    }

    @Test
    public void testInitializationGivenTopicAlreadyExists() throws IOException {
        try (Cache<String, String> cache = createKafkaCacheInstance()) {
            createTopic(bootstrapServers);
            cache.init();
        }
    }

    @Test
    public void testSimplePut() throws Exception {
        try (Cache<String, String> kafkaCache = createKafkaCacheInstance()) {
            createTopic(bootstrapServers);
            kafkaCache.init();
            kafkaCache.put("Kafka", "Rocks");
            fail("Expected put to fail");
        } catch (CacheException e) {
            assertEquals("Cache is read-only", e.getMessage());
        }
    }

    @Test
    public void testSimpleRemove() throws Exception {
        try (Cache<String, String> kafkaCache = createKafkaCacheInstance()) {
            createTopic(bootstrapServers);
            kafkaCache.init();
            kafkaCache.remove("Kafka");
            fail("Expected remove to fail");
        } catch (CacheException e) {
            assertEquals("Cache is read-only", e.getMessage());
        }
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testKeySetIsImmutable() throws Exception {
        try (Cache<String, String> kafkaCache = createKafkaCacheInstance()) {
            createTopic(bootstrapServers);
            kafkaCache.init();
            kafkaCache.keySet().remove("Kafka");
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEntrySetIsImmutable() throws Exception {
        try (Cache<String, String> kafkaCache = createKafkaCacheInstance()) {
            createTopic(bootstrapServers);
            kafkaCache.init();
            kafkaCache.entrySet().add(new AbstractMap.SimpleEntry<>("Kafka", "Rocks"));
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testValuesIsImmutable() throws Exception {
        try (Cache<String, String> kafkaCache = createKafkaCacheInstance()) {
            createTopic(bootstrapServers);
            kafkaCache.init();
            kafkaCache.values().add("Kafka");
        }
    }

    private void createTopic(String bootstrapServers) throws CacheInitializationException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic topicRequest = new NewTopic(DEFAULT_KAFKACACHE_TOPIC, 1, (short) 1);
            topicRequest.configs(
                Collections.singletonMap(
                    TopicConfig.CLEANUP_POLICY_CONFIG,
                    TopicConfig.CLEANUP_POLICY_COMPACT
                )
            );
            admin.createTopics(Collections.singleton(topicRequest)).all().get(1000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new CacheInitializationException("Failed to create topic", e);
        }
    }

    protected Cache<String, String> createKafkaCacheInstance() {
        Properties props = getKafkaCacheProperties();
        KafkaCacheConfig config = new KafkaCacheConfig(props);
        Cache<String, String> kafkaCache = Caches.concurrentCache(
            new KafkaCache<>(config,
                Serdes.String(),
                Serdes.String(),
                new StringUpdateHandler(),
                null));
        return kafkaCache;
    }

    protected Properties getKafkaCacheProperties() {
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_READ_ONLY_CONFIG, true);
        props.put(KafkaCacheConfig.KAFKACACHE_BACKING_CACHE_CONFIG, CacheType.MEMORY.name().toLowerCase());
        return props;
    }
}
