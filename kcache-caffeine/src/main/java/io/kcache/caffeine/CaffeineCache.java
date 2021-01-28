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
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.kcache.CacheLoader;
import io.kcache.utils.InMemoryCache;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;

/**
 * An in-memory cache with bounded size.
 */
public class CaffeineCache<K, V> extends InMemoryCache<K, V> {

    private final com.github.benmanes.caffeine.cache.Cache<K, V> cache;
    private final CacheLoader<K, V> loader;

    public CaffeineCache(Comparator<? super K> comparator) {
        this(null, null, null, comparator);
    }

    public CaffeineCache(
        Integer maximumSize,
        Duration expireAfterWrite,
        CacheLoader<K, V> loader
    ) {
        super();
        this.loader = loader;
        this.cache = createCache(maximumSize, expireAfterWrite);
    }

    public CaffeineCache(
        Integer maximumSize,
        Duration expireAfterWrite,
        CacheLoader<K, V> loader,
        Comparator<? super K> comparator
    ) {
        super(comparator);
        this.loader = loader;
        this.cache = createCache(maximumSize, expireAfterWrite);
    }

    private com.github.benmanes.caffeine.cache.Cache<K, V> createCache(
        Integer maximumSize, Duration expireAfterWrite
    ) {
        Caffeine<K, V> caffeine = Caffeine.newBuilder()
            .writer(new CacheWriter<K, V>() {
                public void write(K key, V value) {
                    delegate().put(key, value);
                }

                public void delete(K key, V value, RemovalCause cause) {
                    delegate().remove(key, value);
                }
            });
        if (maximumSize != null && maximumSize >= 0) {
            caffeine = caffeine.maximumSize(maximumSize);
        }
        if (expireAfterWrite != null && !expireAfterWrite.isNegative()) {
            caffeine = caffeine
                .scheduler(Scheduler.systemScheduler())
                .expireAfterWrite(expireAfterWrite);
        }
        if (loader != null) {
            return caffeine.build(
                key -> {
                    V value = loader.load(key);
                    if (value != null) {
                        delegate().put(key, value);
                    }
                    return value;
                }
            );
        } else {
            return caffeine.build();
        }
    }

    @Override
    public boolean containsKey(final Object key) {
        return get(key) != null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(final Object key) {
        if (loader != null) {
            return ((LoadingCache<K, V>) cache).get((K) key);
        } else {
            return super.get(key);
        }
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