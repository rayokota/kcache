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

package io.kcache;

import io.kcache.exceptions.CacheInitializationException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import org.apache.kafka.common.Configurable;

public interface Cache<K, V> extends SortedMap<K, V>, Configurable, Closeable {

    /**
     * Whether the cache is persistent.
     *
     * @return whether the cache is persistent
     */
    default boolean isPersistent() {
        return false;
    }

    /**
     * Configures the cache.
     */
    default void configure(Map<String, ?> configs) {
    }

    /**
     * Initializes the cache.
     */
    void init() throws CacheInitializationException;

    /**
     * Syncs (or re-initializes) the cache with the backing store.
     */
    void sync();

    /**
     * Returns a view of the portion of this cache whose keys range from
     * {@code fromKey} to {@code toKey}.  If {@code fromKey} and
     * {@code toKey} are equal, the returned cache is empty unless
     * {@code fromInclusive} and {@code toInclusive} are both true.  The
     * returned cache is backed by this cache, so changes in the returned cache are
     * reflected in this cache, and vice-versa.  The returned cache supports all
     * optional cache operations that this cache supports.
     *
     * <p>The returned cache will throw an {@code IllegalArgumentException}
     * on an attempt to insert a key outside of its range, or to construct a
     * subcache either of whose endpoints lie outside its range.
     *
     * @param fromKey       low endpoint of the keys in the returned cache;
     *                      {@code null} indicates the beginning
     * @param fromInclusive {@code true} if the low endpoint
     *                      is to be included in the returned view
     * @param toKey         high endpoint of the keys in the returned cache;
     *                      {@code null} indicates the end
     * @param toInclusive   {@code true} if the high endpoint
     *                      is to be included in the returned view
     * @return a view of the portion of this cache whose keys range from
     * {@code fromKey} to {@code toKey}
     * @throws ClassCastException       if {@code fromKey} and {@code toKey}
     *                                  cannot be compared to one another using this cache's comparator
     *                                  (or, if the cache has no comparator, using natural ordering).
     *                                  Implementations may, but are not required to, throw this
     *                                  exception if {@code fromKey} or {@code toKey}
     *                                  cannot be compared to keys currently in the cache.
     * @throws IllegalArgumentException if {@code fromKey} is greater than
     *                                  {@code toKey}; or if this cache itself has a restricted
     *                                  range, and {@code fromKey} or {@code toKey} lies
     *                                  outside the bounds of the range
     */
    Cache<K, V> subCache(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive);

    /**
     * Returns an iterator over the portion of this cache whose keys range from
     * {@code fromKey} to {@code toKey}.  If {@code fromKey} and
     * {@code toKey} are equal, the returned iterator is empty unless
     * {@code fromInclusive} and {@code toInclusive} are both true.
     *
     * @param fromKey       low endpoint of the keys in the returned iterator;
     *                      {@code null} indicates the beginning
     * @param fromInclusive {@code true} if the low endpoint
     *                      is to be included in the returned view
     * @param toKey         high endpoint of the keys in the returned iterator;
     *                      {@code null} indicates the end
     * @param toInclusive   {@code true} if the high endpoint
     *                      is to be included in the returned view
     * @return an iterator over the portion of this cache whose keys range from
     * {@code fromKey} to {@code toKey}
     * @throws ClassCastException       if {@code fromKey} and {@code toKey}
     *                                  cannot be compared to one another using this cache's comparator
     *                                  (or, if the cache has no comparator, using natural ordering).
     *                                  Implementations may, but are not required to, throw this
     *                                  exception if {@code fromKey} or {@code toKey}
     *                                  cannot be compared to keys currently in the cache.
     * @throws IllegalArgumentException if {@code fromKey} is greater than
     *                                  {@code toKey}; or if this cache itself has a restricted
     *                                  range, and {@code fromKey} or {@code toKey} lies
     *                                  outside the bounds of the range
     */
    KeyValueIterator<K, V> range(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive);

    /**
     * Returns an iterator over all key-value pairs in this cache.
     *
     * @return an <tt>KeyValueIterator</tt> over the elements in this collection
     */
    KeyValueIterator<K, V> all();

    /**
     * Returns a reverse order view of the mappings contained in this cache.
     * The descending cache is backed by this cache, so changes to the cache are
     * reflected in the descending cache, and vice-versa.  If either cache is
     * modified while an iteration over a collection view of either cache
     * is in progress (except through the iterator's own {@code remove}
     * operation), the results of the iteration are undefined.
     *
     * @return a reverse order view of this cache
     */
    Cache<K, V> descendingCache();

    /**
     * Flushes the cache.
     */
    void flush();

    /**
     * Destroys the cache, if persistent.
     */
    void destroy() throws IOException;

    // Default methods

    default SortedMap<K, V> subMap(K fromKey, K toKey) {
        return subCache(fromKey, true, toKey, false);
    }

    default SortedMap<K, V> headMap(K toKey) {
        return subCache(null, false, toKey, false);
    }

    default SortedMap<K, V> tailMap(K fromKey) {
        return subCache(fromKey, true, null, false);
    }
}
