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
import static org.junit.Assert.assertNull;

import io.kcache.exceptions.CacheException;
import io.kcache.utils.ClusterTestHarness;
import java.util.Properties;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaCacheOffsetTest extends ClusterTestHarness {

    private static final Logger log = LoggerFactory.getLogger(KafkaCacheOffsetTest.class);

    @Test
    public void testDifferentOffsetAfterRestart() throws Exception {
        Properties kafkaCacheProps = getKafkaCacheProperties();
        try (Cache<String, String> kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(kafkaCacheProps)) {
            try {
                kafkaCache.put("Kafka", "Rocks");
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store put(Kafka, Rocks) operation failed", e);
            }
            try {
                kafkaCache.put("Kafka2", "Rocks2");
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store put(Kafka2, Rocks2) operation failed", e);
            }
        }
        kafkaCacheProps = getKafkaCacheProperties();
        kafkaCacheProps.put("kafkacache.topic.partitions.offset", "end");
        try (Cache<String, String> kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(kafkaCacheProps)) {
            String retrievedValue;
            try {
                retrievedValue = kafkaCache.get("Kafka");
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertNull("Value should have been deleted", retrievedValue);
            try {
                retrievedValue = kafkaCache.get("Kafka2");
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka2) operation failed", e);
            }
            assertNull("Value should have been deleted", retrievedValue);
        }
        kafkaCacheProps = getKafkaCacheProperties();
        kafkaCacheProps.put("kafkacache.topic.partitions.offset", "1");
        try (Cache<String, String> kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(kafkaCacheProps)) {
            String retrievedValue;
            try {
                retrievedValue = kafkaCache.get("Kafka");
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertNull("Value should have been deleted", retrievedValue);
            try {
                retrievedValue = kafkaCache.get("Kafka2");
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka2) operation failed", e);
            }
            assertEquals("Retrieved value should match entered value", "Rocks2", retrievedValue);
        }
        kafkaCacheProps = getKafkaCacheProperties();
        kafkaCacheProps.put("kafkacache.topic.partitions.offset", "-1");
        try (Cache<String, String> kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(kafkaCacheProps)) {
            String retrievedValue;
            try {
                retrievedValue = kafkaCache.get("Kafka");
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertNull("Value should have been deleted", retrievedValue);
            try {
                retrievedValue = kafkaCache.get("Kafka2");
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka2) operation failed", e);
            }
            assertEquals("Retrieved value should match entered value", "Rocks2", retrievedValue);
        }
        kafkaCacheProps = getKafkaCacheProperties();
        kafkaCacheProps.put("kafkacache.topic.partitions.offset", "@0");
        try (Cache<String, String> kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(kafkaCacheProps)) {
            String retrievedValue;
            try {
                retrievedValue = kafkaCache.get("Kafka");
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertEquals("Retrieved value should match entered value", "Rocks", retrievedValue);
            try {
                retrievedValue = kafkaCache.get("Kafka2");
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka2) operation failed", e);
            }
            assertEquals("Retrieved value should match entered value", "Rocks2", retrievedValue);
        }
        kafkaCacheProps = getKafkaCacheProperties();
        kafkaCacheProps.put("kafkacache.topic.partitions.offset", "@" + Long.MAX_VALUE);
        // Max timestamp causes offsetsForTimes to fail, resulting in seeking to beginning
        try (Cache<String, String> kafkaCache = CacheUtils.createAndInitKafkaCacheInstance(kafkaCacheProps)) {
            String retrievedValue;
            try {
                retrievedValue = kafkaCache.get("Kafka");
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
            }
            assertEquals("Retrieved value should match entered value", "Rocks", retrievedValue);
            try {
                retrievedValue = kafkaCache.get("Kafka2");
            } catch (CacheException e) {
                throw new RuntimeException("Kafka store get(Kafka2) operation failed", e);
            }
            assertEquals("Retrieved value should match entered value", "Rocks2", retrievedValue);
        }
    }

    protected Properties getKafkaCacheProperties() throws Exception {
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KafkaCacheConfig.KAFKACACHE_BACKING_CACHE_CONFIG, CacheType.MEMORY.toString());
        return props;
    }
}
