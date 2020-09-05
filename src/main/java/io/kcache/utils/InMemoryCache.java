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

import com.google.common.collect.ForwardingSortedMap;
import io.kcache.Cache;
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * In-memory cache
 */
public class InMemoryCache<K, V> extends ForwardingSortedMap<K, V> implements Cache<K, V> {

    private final NavigableMap<K, V> delegate;

    public InMemoryCache() {
        // Use a concurrent data structure to ensure fail-safe iterators
        this.delegate = Collections.synchronizedNavigableMap(new TreeMap<>());
    }

    public InMemoryCache(Comparator<? super K> comparator) {
        // Use a concurrent data structure to ensure fail-safe iterators
        this.delegate = Collections.synchronizedNavigableMap(new TreeMap<>(comparator));
    }

    public InMemoryCache(NavigableMap<K, V> delegate) {
        this.delegate = delegate;
    }

    @Override
    protected SortedMap<K, V> delegate() {
        return delegate;
    }

    @Override
    public void init() {
        // do nothing
    }

    @Override
    public void sync() {
        // do nothing
    }

    @Override
    public Cache<K, V> subCache(K from, boolean fromInclusive, K to, boolean toInclusive) {
        return new InMemoryCache<>(subMap(from, fromInclusive, to, toInclusive));
    }

    private NavigableMap<K, V> subMap(K from, boolean fromInclusive, K to, boolean toInclusive) {
        if (from == null) {
            return delegate.headMap(to, toInclusive);
        } else if (to == null) {
            return delegate.tailMap(from, fromInclusive);
        } else {
            return delegate.subMap(from, fromInclusive, to, toInclusive);
        }
    }

    @Override
    public KeyValueIterator<K, V> range(K from, boolean fromInclusive, K to, boolean toInclusive) {
        return new InMemoryKeyValueIterator<>(subMap(from, fromInclusive, to, toInclusive).entrySet().iterator());
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new InMemoryKeyValueIterator<>(entrySet().iterator());
    }

    @Override
    public Cache<K, V> descendingCache() {
        return new InMemoryCache<>(delegate.descendingMap());
    }

    @Override
    public void flush() {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public void destroy() {
        // do nothing
    }

    private static class InMemoryKeyValueIterator<K, V> implements KeyValueIterator<K, V> {
        private final Iterator<Map.Entry<K, V>> iter;

        private InMemoryKeyValueIterator(final Iterator<Map.Entry<K, V>> iter) {
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<K, V> next() {
            final Map.Entry<K, V> entry = iter.next();
            return new KeyValue<>(entry.getKey(), entry.getValue());
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
