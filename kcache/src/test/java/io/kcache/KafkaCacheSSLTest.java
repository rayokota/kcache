/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import io.kcache.exceptions.CacheInitializationException;
import io.kcache.utils.SSLClusterTestHarness;
import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KafkaCacheSSLTest extends SSLClusterTestHarness {
    @Test
    public void testInitialization() throws Exception {
        Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance(requireSSLClientAuth());
        kafkaCache.close();
    }

    @Test(expected = CacheInitializationException.class)
    public void testInitializationWithoutClientAuth() throws Exception {
        Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance(false);
        kafkaCache.close();

        // TODO: make the timeout shorter so the test fails quicker.
    }

    @Test(expected = CacheInitializationException.class)
    public void testDoubleInitialization() throws Exception {
        try (Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance(requireSSLClientAuth())) {
            kafkaCache.init();
        }
    }

    @Test
    public void testSimplePut() throws Exception {
        try (Cache<String, String> kafkaCache = createAndInitKafkaCacheInstance(requireSSLClientAuth())) {
            String key = "Kafka";
            String value = "Rocks";
            kafkaCache.put(key, value);
            String retrievedValue = kafkaCache.get(key);
            assertEquals("Retrieved value should match entered value", value, retrievedValue);
        }
    }

    protected Cache<String, String> createAndInitKafkaCacheInstance(boolean requireSSLClientAuth) {
        Properties props = getKafkaCacheProperties();
        return CacheUtils.createAndInitSSLKafkaCacheInstance(props, clientSslConfigs, requireSSLClientAuth);
    }

    protected Properties getKafkaCacheProperties() {
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KafkaCacheConfig.KAFKACACHE_BACKING_CACHE_CONFIG, CacheType.MEMORY.toString());
        return props;
    }
}
