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

    private Comparator<K> comparator;

    public PersistentCache(Comparator<K> comparator) {
        this.comparator = comparator;
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Override
    public Comparator<? super K> comparator() {
        return comparator;
    }

    @Override
    public Cache<K, V> subCache(K from, boolean fromInclusive, K to, boolean toInclusive) {
        return new SubCache<>(this, from, fromInclusive, to, toInclusive, false);
    }

    @Override
    public Cache<K, V> descendingCache() {
        return new SubCache<>(this, null, false, null, false, true);
    }

    protected abstract KeyValueIterator<K, V> range(K from, boolean fromInclusive, K to, boolean toInclusive, boolean isDescending);

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
            for (Entry<? extends K, ? extends V> e : m.entrySet()) {
                put(e.getKey(), e.getValue());
            }
        }

        public V remove(Object key) {
            return (!inBounds(key, m.comparator)) ? null : m.remove(key);
        }

        public int size() {
            return entrySet().size();
        }

        public boolean isEmpty() {
            return entrySet().isEmpty();
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
            KeyValueIterator<K, V> iter;
            if (isDescending) {
                iter = m.range(hi, hiInclusive, lo, loInclusive, true);
            } else {
                iter = m.range(lo, loInclusive, hi, hiInclusive, false);
            }
            if (!iter.hasNext()) {
                throw new NoSuchElementException();
            }
            return iter.next().key;
        }

        public K lastKey() {
            KeyValueIterator<K, V> iter;
            if (isDescending) {
                iter = m.range(lo, loInclusive, hi, hiInclusive, false);
            } else {
                iter = m.range(hi, hiInclusive, lo, loInclusive, true);
            }
            if (!iter.hasNext()) {
                throw new NoSuchElementException();
            }
            return iter.next().key;
        }

        /* ---------------- Submap Views -------------- */

        public Set<K> keySet() {
            return Streams.streamOf(all())
                .map(kv -> kv.key)
                .collect(Collectors.toCollection(() -> new TreeSet<>(comparator())));
        }

        public Collection<V> values() {
            return Streams.streamOf(all())
                .map(kv -> kv.value)
                .collect(Collectors.toList());
        }

        public Set<Entry<K, V>> entrySet() {
            return Streams.streamOf(all())
                .map(kv -> new AbstractMap.SimpleEntry<>(kv.key, kv.value))
                .collect(Collectors.toCollection(
                    () -> new TreeSet<>((e1, e2) -> comparator().compare(e1.getKey(), e2.getKey()))));
        }

        public KeyValueIterator<K, V> all() {
            if (isDescending) {
                return m.range(hi, hiInclusive, lo, loInclusive, true);
            } else {
                return m.range(lo, loInclusive, hi, hiInclusive, false);
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
