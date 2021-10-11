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

import io.kcache.utils.Caches;
import io.kcache.utils.InMemoryCache;
import io.kcache.utils.StringUpdateHandler;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaDelegatingCacheTest extends KafkaCacheTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaDelegatingCacheTest.class);

    @Override
    protected Cache<String, String> createAndInitKafkaCacheInstance() {
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

        // Create a Kafka cache that delegates to another Kafka cache
        Properties props2 = new Properties();
        props2.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props2.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, "_cache2");
        KafkaCacheConfig config2 = new KafkaCacheConfig(props2);
        Cache<String, String> kafkaDelegatingCache = Caches.concurrentCache(
            new KafkaCache<>(config2,
                Serdes.String(),
                Serdes.String(),
                new StringUpdateHandler(),
                kafkaCache));
        kafkaDelegatingCache.init();
        return kafkaDelegatingCache;
    }
}
