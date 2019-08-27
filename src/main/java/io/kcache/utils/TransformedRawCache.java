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

import io.kcache.Cache;
import io.kcache.KeyValueIterator;
import io.kcache.KeyValueIterators;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A cache which wraps a raw cache and uses serdes to transform the bytes.
 */
public class TransformedRawCache<K, V> implements Cache<K, V> {
    private static final Logger log = LoggerFactory.getLogger(TransformedRawCache.class);

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Cache<Bytes, byte[]> rawCache;

    public TransformedRawCache(Serde<K> keySerde,
                               Serde<V> valueSerde,
                               Cache<Bytes, byte[]> rawCache) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.rawCache = rawCache;
    }

    @Override
    public void init() {
        rawCache.init();
    }

    @Override
    public int size() {
        return rawCache.size();
    }

    @Override
    public boolean isEmpty() {
        return rawCache.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized V put(final K key, final V value) {
        Objects.requireNonNull(key, "key cannot be null");
        final V originalValue = get(key);
        byte[] keyBytes = keySerde.serializer().serialize(null, key);
        byte[] valueBytes = valueSerde.serializer().serialize(null, value);
        rawCache.put(new Bytes(keyBytes), valueBytes);
        return originalValue;
    }

    @Override
    public synchronized V putIfAbsent(final K key, final V value) {
        Objects.requireNonNull(key, "key cannot be null");
        final V originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public synchronized void putAll(Map<? extends K, ? extends V> entries) {
        Map<Bytes, byte[]> rawEntries = entries.entrySet().stream()
            .collect(Collectors.toMap(
                e -> new Bytes(keySerde.serializer().serialize(null, e.getKey())),
                e -> valueSerde.serializer().serialize(null, e.getValue())));
        rawCache.putAll(rawEntries);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized V get(final Object key) {
        byte[] keyBytes = keySerde.serializer().serialize(null, (K) key);
        byte[] valueBytes = rawCache.get(new Bytes(keyBytes));
        return valueSerde.deserializer().deserialize(null, valueBytes);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized V remove(final Object key) {
        Objects.requireNonNull(key, "key cannot be null");
        final V originalValue = get(key);
        put((K) key, null);
        return originalValue;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
        return StreamUtils.streamOf(all())
            .map(kv -> kv.key)
            .collect(Collectors.toSet());
    }

    @Override
    public Collection<V> values() {
        return StreamUtils.streamOf(all())
            .map(kv -> kv.value)
            .collect(Collectors.toList());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return StreamUtils.streamOf(all())
            .map(kv -> new AbstractMap.SimpleEntry<>(kv.key, kv.value))
            .collect(Collectors.toSet());
    }

    @Override
    public synchronized KeyValueIterator<K, V> range(final K from, final K to) {
        Objects.requireNonNull(from, "from cannot be null");
        Objects.requireNonNull(to, "to cannot be null");

        Bytes fromBytes = new Bytes(keySerde.serializer().serialize(null, from));
        Bytes toBytes = new Bytes(keySerde.serializer().serialize(null, to));

        if (fromBytes.compareTo(toBytes) > 0) {
            log.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        final KeyValueIterator<Bytes, byte[]> rocksDBRangeIterator = rawCache.range(fromBytes, toBytes);
        return KeyValueIterators.transformRawIterator(keySerde, valueSerde, rocksDBRangeIterator);
    }

    @Override
    public synchronized KeyValueIterator<K, V> all() {
        final KeyValueIterator<Bytes, byte[]> rocksDBIterator = rawCache.all();
        return KeyValueIterators.transformRawIterator(keySerde, valueSerde, rocksDBIterator);
    }

    @Override
    public synchronized void close() throws IOException {
        rawCache.close();
    }
}
