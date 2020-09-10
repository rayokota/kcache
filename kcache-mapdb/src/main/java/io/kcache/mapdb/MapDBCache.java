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
package io.kcache.mapdb;

import io.kcache.KeyValueIterator;
import io.kcache.KeyValueIterators;
import io.kcache.exceptions.CacheInitializationException;
import io.kcache.utils.PersistentCache;
import java.io.File;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A persistent key-value store based on MapDB.
 */
public class MapDBCache<K, V> extends PersistentCache<K, V> {
    private static final Logger log = LoggerFactory.getLogger(MapDBCache.class);

    private static final String DB_FILE_DIR = "mapdb";

    private DB db;
    private BTreeMap<byte[], byte[]> map;

    public MapDBCache(final String name,
                      final String rootDir,
                      Serde<K> keySerde,
                      Serde<V> valueSerde) {
        this(name, DB_FILE_DIR, rootDir, keySerde, valueSerde);
    }

    public MapDBCache(final String name,
                      final String rootDir,
                      Serde<K> keySerde,
                      Serde<V> valueSerde,
                      Comparator<K> comparator) {
        this(name, DB_FILE_DIR, rootDir, keySerde, valueSerde, comparator);
    }

    public MapDBCache(final String name,
                      final String parentDir,
                      final String rootDir,
                      Serde<K> keySerde,
                      Serde<V> valueSerde) {
        this(name, parentDir, rootDir, keySerde, valueSerde, null);
    }

    public MapDBCache(final String name,
                      final String parentDir,
                      final String rootDir,
                      Serde<K> keySerde,
                      Serde<V> valueSerde,
                      Comparator<K> comparator) {
        super(name, parentDir, rootDir, keySerde, valueSerde, comparator);
    }

    @Override
    protected void openDB() {
        try {
            db = DBMaker.fileDB(new File(dbDir(), "map.db"))
                .fileMmapEnable()
                .make();
            map = db.treeMap(name())
                .keySerializer(new CustomSerializerByteArray<>(keySerde(), comparator()))
                .valueSerializer(Serializer.BYTE_ARRAY)
                .counterEnable()
                .createOrOpen();
        } catch (final Exception e) {
            throw new CacheInitializationException("Error opening store " + name() + " at location " + dbDir(), e);
        }
    }

    @Override
    public int size() {
        validateStoreOpen();
        return map.size();
    }

    @Override
    public V put(final K key, final V value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        byte[] keyBytes = keySerde().serializer().serialize(null, key);
        byte[] valueBytes = valueSerde().serializer().serialize(null, value);
        byte[] oldValueBytes = map.put(keyBytes, valueBytes);
        db.commit();
        return valueSerde().deserializer().deserialize(null, oldValueBytes);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        validateStoreOpen();
        for (Map.Entry<? extends K, ? extends V> entry : entries.entrySet()) {
            byte[] keyBytes = keySerde().serializer().serialize(null, entry.getKey());
            byte[] valueBytes = valueSerde().serializer().serialize(null, entry.getValue());
            map.put(keyBytes, valueBytes);
        }
        db.commit();
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(final Object key) {
        validateStoreOpen();
        byte[] keyBytes = keySerde().serializer().serialize(null, (K) key);
        byte[] valueBytes = map.get(keyBytes);
        return valueSerde().deserializer().deserialize(null, valueBytes);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(final Object key) {
        Objects.requireNonNull(key, "key cannot be null");
        byte[] keyBytes = keySerde().serializer().serialize(null, (K) key);
        byte[] oldValueBytes = map.remove(keyBytes);
        db.commit();
        return valueSerde().deserializer().deserialize(null, oldValueBytes);
    }

    @Override
    protected KeyValueIterator<K, V> range(K from, boolean fromInclusive, K to, boolean toInclusive, boolean isDescending) {
        validateStoreOpen();
        byte[] fromBytes = keySerde().serializer().serialize(null, from);
        byte[] toBytes = keySerde().serializer().serialize(null, to);
        Iterator<Entry<byte[], byte[]>> iter = isDescending
            ? map.descendingEntryIterator(toBytes, toInclusive, fromBytes, fromInclusive)
            : map.entryIterator(fromBytes, fromInclusive, toBytes, toInclusive);
        return KeyValueIterators.transformRawIterator(keySerde(), valueSerde(), iter);
    }

    @Override
    protected KeyValueIterator<K, V> all(boolean isDescending) {
        validateStoreOpen();
        Iterator<Entry<byte[], byte[]>> iter = isDescending
            ? map.descendingEntryIterator()
            : map.entryIterator();
        return KeyValueIterators.transformRawIterator(keySerde(), valueSerde(), iter);
    }

    @Override
    public void flush() {
    }

    @Override
    protected void closeDB() {
        try {
            if (db != null) {
                db.close();
            }
            db = null;
        } catch (Exception e) {
            log.warn("Error during close", e);
        }
    }
}
