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
package io.kcache.lmdb;

import static org.lmdbjava.DbiFlags.MDB_CREATE;

import io.kcache.KeyValueIterator;
import io.kcache.KeyValueIterators;
import io.kcache.exceptions.CacheInitializationException;
import io.kcache.utils.KeyBufferComparator;
import io.kcache.utils.PersistentCache;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A persistent key-value store based on LMDB.
 */
public class LmdbCache<K, V> extends PersistentCache<K, V> {
    private static final Logger log = LoggerFactory.getLogger(LmdbCache.class);

    private static final String DB_FILE_DIR = "lmdb";

    private Env<ByteBuffer> env;
    private Dbi<ByteBuffer> db;

    public LmdbCache(final String name,
                     final String rootDir,
                     Serde<K> keySerde,
                     Serde<V> valueSerde) {
        this(name, DB_FILE_DIR, rootDir, keySerde, valueSerde);
    }

    public LmdbCache(final String name,
                     final String rootDir,
                     Serde<K> keySerde,
                     Serde<V> valueSerde,
                     Comparator<K> comparator) {
        this(name, DB_FILE_DIR, rootDir, keySerde, valueSerde, comparator);
    }

    public LmdbCache(final String name,
                     final String parentDir,
                     final String rootDir,
                     Serde<K> keySerde,
                     Serde<V> valueSerde) {
        this(name, parentDir, rootDir, keySerde, valueSerde, null);
    }

    public LmdbCache(final String name,
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
            env = Env.create()
                .setMapSize(Integer.MAX_VALUE)
                .setMaxDbs(1)
                .setMaxReaders(8)
                .open(dbDir());
            db = env.openDbi(name(), new KeyBufferComparator<>(keySerde(), comparator()), MDB_CREATE);
        } catch (final Exception e) {
            throw new CacheInitializationException("Error opening store " + name() + " at location " + dbDir(), e);
        }
    }

    @Override
    public int size() {
        validateStoreOpen();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            return (int) db.stat(txn).entries;
        }
    }

    @Override
    public V put(final K key, final V value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        final V originalValue = get(key);
        byte[] keyBytes = keySerde().serializer().serialize(null, key);
        byte[] valueBytes = valueSerde().serializer().serialize(null, value);
        ByteBuffer keyBuf = ByteBuffer.allocateDirect(keyBytes.length);
        ByteBuffer valueBuf = ByteBuffer.allocateDirect(valueBytes.length);
        keyBuf.put(keyBytes).flip();
        valueBuf.put(valueBytes).flip();
        db.put(keyBuf, valueBuf);
        return originalValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        validateStoreOpen();
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            for (Map.Entry<? extends K, ? extends V> entry : entries.entrySet()) {
                byte[] keyBytes = keySerde().serializer().serialize(null, entry.getKey());
                byte[] valueBytes = valueSerde().serializer().serialize(null, entry.getValue());
                ByteBuffer keyBuf = ByteBuffer.allocateDirect(keyBytes.length);
                ByteBuffer valueBuf = ByteBuffer.allocateDirect(valueBytes.length);
                keyBuf.put(keyBytes).flip();
                valueBuf.put(valueBytes).flip();
                db.put(txn, keyBuf, valueBuf);
            }
            txn.commit();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(final Object key) {
        validateStoreOpen();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            byte[] keyBytes = keySerde().serializer().serialize(null, (K) key);
            ByteBuffer keyBuf = ByteBuffer.allocateDirect(keyBytes.length);
            keyBuf.put(keyBytes).flip();
            ByteBuffer valueBuf = db.get(txn, keyBuf);
            if (valueBuf == null) {
                return null;
            }
            byte[] valueBytes = new byte[valueBuf.remaining()];
            valueBuf.get(valueBytes);
            return valueSerde().deserializer().deserialize(null, valueBytes);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(final Object key) {
        Objects.requireNonNull(key, "key cannot be null");
        final V originalValue = get(key);
        byte[] keyBytes = keySerde().serializer().serialize(null, (K) key);
        ByteBuffer keyBuf = ByteBuffer.allocateDirect(keyBytes.length);
        keyBuf.put(keyBytes).flip();
        db.delete(keyBuf);
        return originalValue;
    }

    @Override
    protected KeyValueIterator<K, V> range(K from, boolean fromInclusive, K to, boolean toInclusive, boolean isDescending) {
        byte[] fromBytes = keySerde().serializer().serialize(null, from);
        byte[] toBytes = keySerde().serializer().serialize(null, to);
        ByteBuffer fromBuf = null;
        ByteBuffer toBuf = null;
        if (fromBytes != null) {
            fromBuf = ByteBuffer.allocateDirect(fromBytes.length);
            fromBuf.put(fromBytes).flip();
        }
        if (toBytes != null) {
            toBuf = ByteBuffer.allocateDirect(toBytes.length);
            toBuf.put(toBytes).flip();
        }

        validateStoreOpen();

        KeyRange<ByteBuffer> keyRange;
        if (isDescending) {
            if (from != null) {
                if (to != null) {
                    if (fromInclusive) {
                        if (toInclusive) {
                            keyRange = KeyRange.closedBackward(fromBuf, toBuf);
                        } else {
                            keyRange = KeyRange.closedOpenBackward(fromBuf, toBuf);
                        }
                    } else {
                        if (toInclusive) {
                            keyRange = KeyRange.openClosedBackward(fromBuf, toBuf);
                        } else {
                            keyRange = KeyRange.openBackward(fromBuf, toBuf);
                        }
                    }
                } else {
                    if (fromInclusive) {
                        keyRange = KeyRange.atLeastBackward(fromBuf);
                    } else {
                        keyRange = KeyRange.greaterThanBackward(fromBuf);
                    }
                }
            } else {
                if (to != null) {
                    if (toInclusive) {
                        keyRange = KeyRange.atMostBackward(toBuf);
                    } else {
                        keyRange = KeyRange.lessThanBackward(toBuf);
                    }
                } else {
                    keyRange = KeyRange.allBackward();
                }
            }
        } else {
            if (from != null) {
                if (to != null) {
                    if (fromInclusive) {
                        if (toInclusive) {
                            keyRange = KeyRange.closed(fromBuf, toBuf);
                        } else {
                            keyRange = KeyRange.closedOpen(fromBuf, toBuf);
                        }
                    } else {
                        if (toInclusive) {
                            keyRange = KeyRange.openClosed(fromBuf, toBuf);
                        } else {
                            keyRange = KeyRange.open(fromBuf, toBuf);
                        }
                    }
                } else {
                    if (fromInclusive) {
                        keyRange = KeyRange.atLeast(fromBuf);
                    } else {
                        keyRange = KeyRange.greaterThan(fromBuf);
                    }
                }
            } else {
                if (to != null) {
                    if (toInclusive) {
                        keyRange = KeyRange.atMost(toBuf);
                    } else {
                        keyRange = KeyRange.lessThan(toBuf);
                    }
                } else {
                    keyRange = KeyRange.all();
                }
            }
        }
        final KeyValueIterator<byte[], byte[]> lmdbIterator = new LmdbIterator(env, db, keyRange);
        return KeyValueIterators.transformRawIterator(keySerde(), valueSerde(), lmdbIterator);
    }

    @Override
    protected KeyValueIterator<K, V> all(boolean isDescending) {
        validateStoreOpen();
        KeyRange<ByteBuffer> keyRange = isDescending ? KeyRange.allBackward() : KeyRange.all();
        final KeyValueIterator<byte[], byte[]> lmdbIterator = new LmdbIterator(env, db, keyRange);
        return KeyValueIterators.transformRawIterator(keySerde(), valueSerde(), lmdbIterator);
    }

    @Override
    public void flush() {
        if (db == null) {
            return;
        }
        env.sync(true);
    }

    @Override
    protected void closeDB() {
        db.close();
        env.close();

        db = null;
        env = null;
    }
}
