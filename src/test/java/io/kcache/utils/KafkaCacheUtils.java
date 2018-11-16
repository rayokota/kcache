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

package io.kcache.utils;

import io.kcache.Cache;
import io.kcache.KafkaCacheConfig;
import io.kcache.KafkaCache;
import io.kcache.exceptions.CacheInitializationException;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

/**
 * For all cache related utility methods.
 */
public class KafkaCacheUtils {

    /**
     * Get a new instance of KafkaCache and initialize it.
     */
    public static KafkaCache<String, String> createAndInitKafkaCacheInstance(String bootstrapServers)
        throws CacheInitializationException {
        Cache<String, String> inMemoryCache = new InMemoryCache<>();
        return createAndInitKafkaCacheInstance(bootstrapServers, inMemoryCache);
    }

    /**
     * Get a new instance of KafkaCache and initialize it.
     */
    public static KafkaCache<String, String> createAndInitKafkaCacheInstance(
        String bootstrapServers, Cache<String, String> inMemoryCache)
        throws CacheInitializationException {
        return createAndInitKafkaCacheInstance(bootstrapServers, inMemoryCache,
            new Properties());
    }

    /**
     * Get a new instance of KafkaCache and initialize it.
     */
    public static KafkaCache<String, String> createAndInitKafkaCacheInstance(
        String bootstrapServers, Cache<String, String> inMemoryCache,
        Properties props)
        throws CacheInitializationException {
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        KafkaCacheConfig config = new KafkaCacheConfig(props);

        KafkaCache<String, String> kafkaCache =
            new KafkaCache<>(config,
                Serdes.String(),
                Serdes.String(),
                new StringUpdateHandler(),
                inMemoryCache);
        kafkaCache.init();
        return kafkaCache;
    }
}
