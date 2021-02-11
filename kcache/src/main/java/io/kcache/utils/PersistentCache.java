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

import com.google.common.primitives.SignedBytes;
import io.kcache.Cache;
import io.kcache.KeyValueIterator;
import io.kcache.exceptions.CacheException;
import io.kcache.exceptions.CacheInitializationException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * A persistent key-value store.
 */
public abstract class PersistentCache<K, V> implements Cache<K, V> {
    private static final Logger log = LoggerFactory.getLogger(PersistentCache.class);

    public static final String MOVEME_FILE_NAME = "moveme";

    private static final Comparator<byte[]> BYTES_COMPARATOR = SignedBytes.lexicographicalComparator();

    private final String name;
    private final String parentDir;
    private final String rootDir;
    private final File dbDir;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Comparator<K> comparator;

    private volatile boolean open = false;

    public PersistentCache(final String name,
                           final String parentDir,
                           final String rootDir,
                           Serde<K> keySerde,
                           Serde<V> valueSerde,
                           Comparator<K> comparator) {
        this.name = name;
        this.parentDir = parentDir;
        this.rootDir = rootDir;
        this.dbDir = new File(new File(rootDir, parentDir), name);
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.comparator = comparator != null
            ? comparator
            : new KeyComparator<>(keySerde, BYTES_COMPARATOR);
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    public String name() {
        return name;
    }

    public File dbDir() {
        return dbDir;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    @Override
    public Comparator<? super K> comparator() {
        return comparator;
    }

    @Override
    public synchronized void init() {
        try {
            File moveme = new File(rootDir, MOVEME_FILE_NAME);
            if (moveme.exists()) {
                Files.move(Paths.get(rootDir), Paths.get(rootDir + ".bak"),
                    StandardCopyOption.REPLACE_EXISTING);
                Files.delete(Paths.get(rootDir + ".bak", MOVEME_FILE_NAME));
            }
            Files.createDirectories(dbDir.getParentFile().toPath());
            Files.createDirectories(dbDir.getAbsoluteFile().toPath());
        } catch (final IOException fatal) {
            throw new CacheInitializationException("Could not create directories", fatal);
        }

        // open the DB dir
        openDB();
        open = true;
    }

    protected abstract void openDB();

    @Override
    public void sync() {
        // do nothing
    }

    protected void validateStoreOpen() {
        if (!open) {
            throw new CacheException("Cache is currently closed");
        }
    }

    @Override
    public boolean isEmpty() {
        validateStoreOpen();
        try (KeyValueIterator<K, V> iter = all()) {
            return iter.hasNext();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        validateStoreOpen();
        return get(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        // Threads accessing this method should use external synchronization
        // See https://github.com/facebook/rocksdb/issues/433
        Objects.requireNonNull(key, "key cannot be null");
        final V originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public Cache<K, V> subCache(K from, boolean fromInclusive, K to, boolean toInclusive) {
        return new SubCache<>(this, from, fromInclusive, to, toInclusive, false);
    }

    @Override
    public Cache<K, V> descendingCache() {
        return new SubCache<>(this, null, false, null, false, true);
    }

    @Override
    public Set<K> keySet() {
        try (KeyValueIterator<K, V> iter = all()) {
            return Streams.streamOf(iter)
                .map(kv -> kv.key)
                .collect(Collectors.toCollection(() -> new TreeSet<>(comparator())));
        }
    }

    @Override
    public Collection<V> values() {
        try (KeyValueIterator<K, V> iter = all()) {
            return Streams.streamOf(iter)
                .map(kv -> kv.value)
                .collect(Collectors.toList());
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        try (KeyValueIterator<K, V> iter = all()) {
            return Streams.streamOf(iter)
                .map(kv -> new AbstractMap.SimpleEntry<>(kv.key, kv.value))
                .collect(Collectors.toCollection(
                    () -> new TreeSet<>(
                        (e1, e2) -> comparator().compare(e1.getKey(), e2.getKey()))));
        }
    }

    @Override
    public K firstKey() {
        try (KeyValueIterator<K, V> iter = all(false)) {
            if (!iter.hasNext()) {
                throw new NoSuchElementException();
            }
            return iter.next().key;
        }
    }

    @Override
    public K lastKey() {
        try (KeyValueIterator<K, V> iter = all(true)) {
            if (!iter.hasNext()) {
                throw new NoSuchElementException();
            }
            return iter.next().key;
        }
    }

    @Override
    public KeyValueIterator<K, V> range(K from, boolean fromInclusive, K to, boolean toInclusive) {
        return range(from, fromInclusive, to, toInclusive, false);
    }

    protected abstract KeyValueIterator<K, V> range(
        K from, boolean fromInclusive, K to, boolean toInclusive, boolean isDescending);

    @Override
    public KeyValueIterator<K, V> all() {
        return all(false);
    }

    protected abstract KeyValueIterator<K, V> all(boolean isDescending);

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void close() {
        if (!open) {
            return;
        }

        open = false;
        closeDB();
    }

    protected abstract void closeDB();

    @Override
    public synchronized void destroy() throws IOException {
        Utils.delete(dbDir());
    }

    /**
     * Compares using comparator or natural ordering if null.
     * Called only by methods that have performed required type checks.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static int cpr(Comparator c, Object x, Object y) {
        return (c != null) ? c.compare(x, y) : ((Comparable) x).compareTo(y);
    }

    static final class SubCache<K, V> implements Cache<K, V> {

        /**
         * Underlying cache
         */
        private final PersistentCache<K, V> m;
        /**
         * lower bound key, or null if from start
         */
        private final K lo;
        /**
         * upper bound key, or null if to end
         */
        private final K hi;
        /**
         * inclusion flag for lo
         */
        private final boolean loInclusive;
        /**
         * inclusion flag for hi
         */
        private final boolean hiInclusive;
        /**
         * direction
         */
        private final boolean isDescending;

        /**
         * Creates a new submap, initializing all fields.
         */
        SubCache(PersistentCache<K, V> map,
                 K fromKey, boolean fromInclusive,
                 K toKey, boolean toInclusive,
                 boolean isDescending) {
            Comparator<? super K> cmp = map.comparator;
            if (fromKey != null && toKey != null &&
                cpr(cmp, fromKey, toKey) > 0)
                throw new IllegalArgumentException("inconsistent range");
            this.m = map;
            this.lo = fromKey;
            this.hi = toKey;
            this.loInclusive = fromInclusive;
            this.hiInclusive = toInclusive;
            this.isDescending = isDescending;
        }

        public void init() {
        }

        public void sync() {
        }

        public void flush() {
        }

        public void close() {
        }

        public void destroy() {
        }

        /* ----------------  Utilities -------------- */

        boolean tooLow(Object key, Comparator<? super K> cmp) {
            int c;
            return (lo != null && ((c = cpr(cmp, key, lo)) < 0 ||
                (c == 0 && !loInclusive)));
        }

        boolean tooHigh(Object key, Comparator<? super K> cmp) {
            int c;
            return (hi != null && ((c = cpr(cmp, key, hi)) > 0 ||
                (c == 0 && !hiInclusive)));
        }

        boolean inBounds(Object key, Comparator<? super K> cmp) {
            return !tooLow(key, cmp) && !tooHigh(key, cmp);
        }

        void checkKeyBounds(K key, Comparator<? super K> cmp) {
            if (key == null)
                throw new NullPointerException();
            if (!inBounds(key, cmp))
                throw new IllegalArgumentException("key out of range");
        }

        /* ----------------  Map API methods -------------- */

        public boolean containsKey(Object key) {
            if (key == null) throw new NullPointerException();
            return inBounds(key, m.comparator) && m.containsKey(key);
        }

        public V get(Object key) {
            if (key == null) throw new NullPointerException();
            return (!inBounds(key, m.comparator)) ? null : m.get(key);
        }

        public V put(K key, V value) {
            checkKeyBounds(key, m.comparator);
            return m.put(key, value);
        }

        public void putAll(Map<? extends K, ? extends V> entries) {
            for (Entry<? extends K, ? extends V> e : entries.entrySet()) {
                put(e.getKey(), e.getValue());
            }
        }

        public V remove(Object key) {
            return (!inBounds(key, m.comparator)) ? null : m.remove(key);
        }

        public int size() {
            try (KeyValueIterator<K, V> iter = all()) {
                return (int) Streams.streamOf(iter).count();
            }
        }

        public boolean isEmpty() {
            try (KeyValueIterator<K, V> iter = all()) {
                return iter.hasNext();
            }
        }

        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException();
        }

        public void clear() {
            throw new UnsupportedOperationException();
        }

        /* ----------------  ConcurrentMap API methods -------------- */

        public V putIfAbsent(K key, V value) {
            checkKeyBounds(key, m.comparator);
            return m.putIfAbsent(key, value);
        }

        public boolean remove(Object key, Object value) {
            return inBounds(key, m.comparator) && m.remove(key, value);
        }

        public boolean replace(K key, V oldValue, V newValue) {
            checkKeyBounds(key, m.comparator);
            return m.replace(key, oldValue, newValue);
        }

        public V replace(K key, V value) {
            checkKeyBounds(key, m.comparator);
            return m.replace(key, value);
        }

        /* ----------------  SortedMap API methods -------------- */

        public Comparator<? super K> comparator() {
            Comparator<? super K> cmp = m.comparator();
            if (isDescending)
                return Collections.reverseOrder(cmp);
            else
                return cmp;
        }

        /**
         * Utility to create submaps, where given bounds override
         * unbounded(null) ones and/or are checked against bounded ones.
         */
        SubCache<K, V> newSubCache(K fromKey, boolean fromInclusive,
                                   K toKey, boolean toInclusive) {
            Comparator<? super K> cmp = m.comparator;
            if (isDescending) { // flip senses
                K tk = fromKey;
                fromKey = toKey;
                toKey = tk;
                boolean ti = fromInclusive;
                fromInclusive = toInclusive;
                toInclusive = ti;
            }
            if (lo != null) {
                if (fromKey == null) {
                    fromKey = lo;
                    fromInclusive = loInclusive;
                } else {
                    int c = cpr(cmp, fromKey, lo);
                    if (c < 0 || (c == 0 && !loInclusive && fromInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            if (hi != null) {
                if (toKey == null) {
                    toKey = hi;
                    toInclusive = hiInclusive;
                } else {
                    int c = cpr(cmp, toKey, hi);
                    if (c > 0 || (c == 0 && !hiInclusive && toInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            return new SubCache<>(m, fromKey, fromInclusive,
                toKey, toInclusive, isDescending);
        }

        public SubCache<K, V> subCache(K fromKey, boolean fromInclusive,
                                       K toKey, boolean toInclusive) {
            return newSubCache(fromKey, fromInclusive, toKey, toInclusive);
        }

        public SubCache<K, V> descendingCache() {
            return new SubCache<K, V>(m, lo, loInclusive,
                hi, hiInclusive, !isDescending);
        }

        /* ----------------  Relational methods -------------- */

        public K firstKey() {
            try (KeyValueIterator<K, V> iter = all(false)) {
                if (!iter.hasNext()) {
                    throw new NoSuchElementException();
                }
                return iter.next().key;
            }
        }

        public K lastKey() {
            try (KeyValueIterator<K, V> iter = all(true)) {
                if (!iter.hasNext()) {
                    throw new NoSuchElementException();
                }
                return iter.next().key;
            }
        }

        /* ---------------- Submap Views -------------- */

        public Set<K> keySet() {
            try (KeyValueIterator<K, V> iter = all()) {
                return Streams.streamOf(iter)
                    .map(kv -> kv.key)
                    .collect(Collectors.toCollection(() -> new TreeSet<>(comparator())));
            }
        }

        public Collection<V> values() {
            try (KeyValueIterator<K, V> iter = all()) {
                return Streams.streamOf(iter)
                    .map(kv -> kv.value)
                    .collect(Collectors.toList());
            }
        }

        public Set<Entry<K, V>> entrySet() {
            try (KeyValueIterator<K, V> iter = all()) {
                return Streams.streamOf(iter)
                    .map(kv -> new AbstractMap.SimpleEntry<>(kv.key, kv.value))
                    .collect(Collectors.toCollection(
                        () -> new TreeSet<>(
                            (e1, e2) -> comparator().compare(e1.getKey(), e2.getKey()))));
            }
        }

        public KeyValueIterator<K, V> all() {
            return all(false);
        }

        KeyValueIterator<K, V> all(boolean isDescending) {
            if (isDescending == this.isDescending) {
                return m.range(lo, loInclusive, hi, hiInclusive, false);
            } else {
                return m.range(hi, hiInclusive, lo, loInclusive, true);
            }
        }

        public KeyValueIterator<K, V> range(K fromKey, boolean fromInclusive,
                                            K toKey, boolean toInclusive) {
            Comparator<? super K> cmp = m.comparator;
            if (lo != null) {
                if (fromKey == null) {
                    fromKey = lo;
                    fromInclusive = loInclusive;
                } else {
                    int c = cpr(cmp, fromKey, lo);
                    if (c < 0 || (c == 0 && !loInclusive && fromInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            if (hi != null) {
                if (toKey == null) {
                    toKey = hi;
                    toInclusive = hiInclusive;
                } else {
                    int c = cpr(cmp, toKey, hi);
                    if (c > 0 || (c == 0 && !hiInclusive && toInclusive))
                        throw new IllegalArgumentException("key out of range");
                }
            }
            return m.range(fromKey, fromInclusive, toKey, toInclusive, isDescending);
        }
    }
}
