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

package io.kcache.caffeine;

import static org.junit.Assert.assertEquals;

import io.kcache.Cache;
import io.kcache.CacheLoader;
import io.kcache.CacheType;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.KafkaCacheTest;
import io.kcache.utils.Caches;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

public class KafkaCaffeineCacheTest extends KafkaCacheTest {

    @Test
    public void testCacheLoader() throws Exception {
        try (Cache<String, String> kafkaCache = createAndInitKafkaCacheInstanceWithLoader()) {
            String key = "Kafka";
            String value = "Rocks";
            kafkaCache.put(key, value);
            String key2 = "Kafka2";
            String value2 = "Rocks2";
            kafkaCache.put(key2, value2);
            String retrievedValue = kafkaCache.get(key);
            assertEquals("Retrieved value should match entered value", value, retrievedValue);
        }
    }

    private Cache<String, String> createAndInitKafkaCacheInstanceWithLoader() throws Exception {
        Properties props = super.getKafkaCacheProperties();
        KafkaCacheConfig config = new KafkaCacheConfig(props);
        Map<String, String> map = new ConcurrentHashMap<>();
        CaffeineCache<String, String> cache = new CaffeineCache<>(
            1, null, new StringFromMapCacheLoader(map));
        Cache<String, String> kafkaCache = Caches.concurrentCache(
            new KafkaCache<>(config,
                Serdes.String(),
                Serdes.String(),
                new StringToMapUpdateHandler(map),
                cache));
        kafkaCache.init();
        return kafkaCache;
    }

    public static class StringToMapUpdateHandler implements CacheUpdateHandler<String, String> {
        public final Map<String, String> map;

        public StringToMapUpdateHandler(Map<String, String> map) {
            this.map = map;
        }

        @Override
        public void handleUpdate(String key, String value, String oldValue,
            TopicPartition tp, long offset, long timestamp) {
            map.put(key, value);
        }
    }

    public static class StringFromMapCacheLoader implements CacheLoader<String, String> {
        public final Map<String, String> map;

        public StringFromMapCacheLoader(Map<String, String> map) {
            this.map = map;
        }

        @Override
        public String load(String key) {
            return map.get(key);
        }
    }

    @Override
    protected Properties getKafkaCacheProperties() throws Exception {
        Properties props = super.getKafkaCacheProperties();
        props.put(KafkaCacheConfig.KAFKACACHE_BACKING_CACHE_CONFIG, CacheType.CAFFEINE.toString());
        props.put(KafkaCacheConfig.KAFKACACHE_BOUNDED_CACHE_SIZE_CONFIG, 1);
        return props;
    }
}
