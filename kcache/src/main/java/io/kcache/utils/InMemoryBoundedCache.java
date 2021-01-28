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
package io.kcache.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import io.kcache.CacheLoader;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;

/**
 * An in-memory cache with bounded size.
 */
public class InMemoryBoundedCache<K, V> extends InMemoryCache<K, V> {

    private final com.google.common.cache.Cache<K, V> cache;
    private final CacheLoader<K, V> loader;

    public InMemoryBoundedCache(Comparator<? super K> comparator) {
        this(null, null, null, comparator);
    }

    public InMemoryBoundedCache(
        Integer maximumSize,
        Duration expireAfterWrite,
        CacheLoader<K, V> loader
    ) {
        super();
        this.loader = loader;
        this.cache = createCache(maximumSize, expireAfterWrite);
    }

    public InMemoryBoundedCache(
        Integer maximumSize,
        Duration expireAfterWrite,
        CacheLoader<K, V> loader,
        Comparator<? super K> comparator
    ) {
        super(comparator);
        this.loader = loader;
        this.cache = createCache(maximumSize, expireAfterWrite);
    }

    @SuppressWarnings("unchecked")
    private com.google.common.cache.Cache<K, V> createCache(
        Integer maximumSize, Duration expireAfterWrite
    ) {
        CacheBuilder<K, V> cacheBuilder = CacheBuilder.newBuilder()
            .removalListener(new RemovalListener<K, V>() {
                @Override
                public void onRemoval(RemovalNotification<K, V> entry) {
                    delegate().remove(entry.getKey(), entry.getValue());
                }
            });
        if (maximumSize != null && maximumSize >= 0) {
            cacheBuilder = cacheBuilder.maximumSize(maximumSize);
        }
        if (expireAfterWrite != null && !expireAfterWrite.isNegative()) {
            cacheBuilder = cacheBuilder.expireAfterWrite(expireAfterWrite);
        }
        if (loader != null) {
            return cacheBuilder.build(
                new com.google.common.cache.CacheLoader<K, V>() {
                    @Override
                    public V load(K key) throws Exception {
                        V value = loader.load(key);
                        if (value != null) {
                            delegate().put(key, value);
                        }
                        return value;
                    }
                }
            );
        } else {
            return cacheBuilder.build();
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
            try {
                return ((LoadingCache<K, V>) cache).getUnchecked((K) key);
            } catch (InvalidCacheLoadException e) {
                return null;
            }
        } else {
            return super.get(key);
        }
    }

    @Override
    public V put(final K key, final V value) {
        cache.put(key, value);
        return super.put(key, value);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        cache.putAll(entries);
        super.putAll(entries);
    }

    @Override
    public V remove(final Object key) {
        cache.invalidate(key);
        return super.remove(key);
    }

    @Override
    public void clear() {
        cache.invalidateAll();
        super.clear();
    }
}