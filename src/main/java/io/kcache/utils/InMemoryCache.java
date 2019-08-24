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
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory cache
 */
public class InMemoryCache<K, V> extends ConcurrentSkipListMap<K, V> implements Cache<K, V> {

    @Override
    public void init() {
        // do nothing
    }

    @Override
    public KeyValueIterator<K, V> range(final K from, final K to) {
        return new InMemoryKeyValueIterator<>(subMap(from, true, to, true).entrySet().iterator());
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new InMemoryKeyValueIterator<>(entrySet().iterator());
    }

    @Override
    public void close() {
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
