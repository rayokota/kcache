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
import io.kcache.utils.Caches;
import io.kcache.utils.StringUpdateHandler;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Map;
import java.util.Properties;

public class CacheUtils {

    /**
     * Get a new instance of a KafkaCache and initialize it.
     */
    public static Cache<String, String> createAndInitKafkaCacheInstance(Properties props)
        throws CacheInitializationException {
        KafkaCacheConfig config = new KafkaCacheConfig(props);
        Cache<String, String> kafkaCache = Caches.concurrentCache(
            new KafkaCache<>(config,
                Serdes.String(),
                Serdes.String(),
                new StringUpdateHandler(),
                null));
        kafkaCache.init();
        return kafkaCache;
    }
    /**
     * Get a new instance of an SASL KafkaCache and initialize it.
     */
    public static Cache<String, String> createAndInitSASLCacheInstance(Properties props)
        throws CacheInitializationException {

        props.put(KafkaCacheConfig.KAFKACACHE_SECURITY_PROTOCOL_CONFIG,
            SecurityProtocol.SASL_PLAINTEXT.toString());

        return createAndInitKafkaCacheInstance(props);
    }

    /**
     * Get a new instance of an SSL KafkaCache and initialize it.
     */
    public static Cache<String, String> createAndInitSSLKafkaCacheInstance(
        Properties props, Map<String, Object> sslConfigs, boolean requireSSLClientAuth)
        throws CacheInitializationException {

        props.put(KafkaCacheConfig.KAFKACACHE_SECURITY_PROTOCOL_CONFIG,
            SecurityProtocol.SSL.toString());
        props.put(KafkaCacheConfig.KAFKACACHE_SSL_TRUSTSTORE_LOCATION_CONFIG,
            sslConfigs.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        props.put(KafkaCacheConfig.KAFKACACHE_SSL_TRUSTSTORE_PASSWORD_CONFIG,
            ((Password) sslConfigs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)).value());
        if (requireSSLClientAuth) {
            props.put(KafkaCacheConfig.KAFKACACHE_SSL_KEYSTORE_LOCATION_CONFIG,
                sslConfigs.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
            props.put(KafkaCacheConfig.KAFKACACHE_SSL_KEYSTORE_PASSWORD_CONFIG,
                ((Password) sslConfigs.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)).value());
            props.put(KafkaCacheConfig.KAFKACACHE_SSL_KEY_PASSWORD_CONFIG,
                ((Password) sslConfigs.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG)).value());
        }

        return createAndInitKafkaCacheInstance(props);
    }
}
