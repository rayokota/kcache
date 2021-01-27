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
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;

/**
 * An in-memory cache with bounded size.
 */
public class InMemoryBoundedCache<K, V> extends InMemoryCache<K, V> {

    private final com.google.common.cache.Cache<K, V> cache;

    public InMemoryBoundedCache(Comparator<? super K> comparator) {
        this(null, null, comparator);
    }

    public InMemoryBoundedCache(Integer maximumSize, Duration expireAfterWrite) {
        super();
        this.cache = createCache(maximumSize, expireAfterWrite);
    }

    public InMemoryBoundedCache(Integer maximumSize, Duration expireAfterWrite,
        Comparator<? super K> comparator) {
        super(comparator);
        this.cache = createCache(maximumSize, expireAfterWrite);
    }

    @SuppressWarnings("unchecked")
    private com.google.common.cache.Cache<K, V> createCache(
        Integer maximumSize, Duration expireAfterWrite) {
        CacheBuilder<?, ?> cacheBuilder = CacheBuilder.newBuilder();
        if (maximumSize != null && maximumSize >= 0) {
            cacheBuilder = cacheBuilder.maximumSize(maximumSize);
        }
        if (expireAfterWrite != null && !expireAfterWrite.isNegative()) {
            cacheBuilder = cacheBuilder.expireAfterWrite(expireAfterWrite);
        }
        return (com.google.common.cache.Cache<K, V>) cacheBuilder.build();
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