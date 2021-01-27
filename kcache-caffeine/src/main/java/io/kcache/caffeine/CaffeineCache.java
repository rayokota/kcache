/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kcache.caffeine;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.kcache.utils.InMemoryCache;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;

/**
 * An in-memory cache with bounded size.
 */
public class CaffeineCache<K, V> extends InMemoryCache<K, V> {

    private final com.github.benmanes.caffeine.cache.Cache<K, V> cache;

    public CaffeineCache(Long maximumSize, Duration expireAfterWrite) {
        super();
        this.cache = createCache(maximumSize, expireAfterWrite);
    }

    public CaffeineCache(Comparator<? super K> comparator,
        Long maximumSize, Duration expireAfterWrite) {
        super(comparator);
        this.cache = createCache(maximumSize, expireAfterWrite);
    }

    public CaffeineCache(NavigableMap<K, V> delegate, Long maximumSize, Duration expireAfterWrite) {
        super(delegate);
        this.cache = createCache(maximumSize, expireAfterWrite);
    }

    private com.github.benmanes.caffeine.cache.Cache<K, V> createCache(
        Long maximumSize, Duration expireAfterWrite) {
        Caffeine<K, V> caffeine = Caffeine.newBuilder()
            .writer(new CacheWriter<K, V>() {
                public void write(K key, V value) {
                    delegate().put(key, value);
                }

                public void delete(K key, V value, RemovalCause cause) {
                    delegate().remove(key, value);
                }
            });
        if (maximumSize != null) {
            if (maximumSize <= 0) {
                throw new IllegalArgumentException("Maximum size of cache must be positive");
            }
            caffeine = caffeine.maximumSize(maximumSize);
        }
        if (expireAfterWrite != null) {
            caffeine = caffeine
                .scheduler(Scheduler.systemScheduler())
                .expireAfterWrite(expireAfterWrite);
        }
        return caffeine.build();
    }

    @Override
    public V put(final K key, final V value) {
        final V originalValue = get(key);
        cache.put(key, value);
        return originalValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        cache.putAll(entries);
    }

    @Override
    public V remove(final Object key) {
        final V originalValue = get(key);
        cache.invalidate(key);
        return originalValue;
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }
}