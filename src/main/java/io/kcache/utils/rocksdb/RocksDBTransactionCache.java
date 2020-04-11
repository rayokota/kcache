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
package io.kcache.utils.rocksdb;

import io.kcache.KeyValueIterator;
import io.kcache.KeyValueIterators;
import io.kcache.Transaction;
import io.kcache.TransactionCache;
import io.kcache.exceptions.CacheException;
import io.kcache.exceptions.CacheInitializationException;
import org.apache.kafka.common.serialization.Serde;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A persistent key-value store based on TransactionDB.
 */
public class RocksDBTransactionCache<K, V> extends RocksDBCache<K, V> implements TransactionCache<K, V> {
    private static final Logger log = LoggerFactory.getLogger(RocksDBTransactionCache.class);

    private RocksDBTransactionAccessor dbAccessor;

    public RocksDBTransactionCache(final String name,
                                   final String rootDir,
                                   Serde<K> keySerde,
                                   Serde<V> valueSerde) {
        super(name, rootDir, keySerde, valueSerde);
    }

    public RocksDBTransactionCache(final String name,
                                   final String parentDir,
                                   final String rootDir,
                                   Serde<K> keySerde,
                                   Serde<V> valueSerde) {
        super(name, parentDir, rootDir, keySerde, valueSerde);
    }

    public RocksDBTransactionCache(final String name,
                                   final String parentDir,
                                   final String rootDir,
                                   Serde<K> keySerde,
                                   Serde<V> valueSerde,
                                   Comparator<K> comparator) {
        super(name, parentDir, rootDir, keySerde, valueSerde, comparator);
    }

    @Override
    protected TransactionDB getDB() {
        return (TransactionDB) super.getDB();
    }

    @Override
    protected TransactionDB open(final DBOptions dbOptions,
                                 List<ColumnFamilyDescriptor> columnFamilyDescriptors,
                                 List<ColumnFamilyHandle> columnFamilies) {
        dbAccessor = new SingleColumnFamilyAccessor(columnFamilies.get(0));

        try {
            TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
            return TransactionDB.open(dbOptions, transactionDBOptions, getDbDir().getAbsolutePath(),
                columnFamilyDescriptors, columnFamilies);
        } catch (final RocksDBException e) {
            throw new CacheInitializationException("Error opening store " + getName() + " at location " + getDbDir().toString(), e);
        }
    }

    @Override
    public boolean containsKey(K key, Transaction tx) {
        validateStoreOpen();
        return get(key, tx) != null;
    }

    @Override
    public V get(K key, Transaction tx) {
        validateStoreOpen();
        try {
            byte[] keyBytes = getKeySerde().serializer().serialize(null, (K) key);
            byte[] valueBytes = dbAccessor.get(keyBytes, ((RocksDBTransaction) tx).getTx());
            return getValueSerde().deserializer().deserialize(null, valueBytes);
        } catch (final RocksDBException e) {
            // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
            throw new CacheException("Error while getting value for key from store " + getName(), e);
        }
    }

    @Override
    public V put(K key, V value, Transaction tx) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        final V originalValue = get(key);
        byte[] keyBytes = getKeySerde().serializer().serialize(null, key);
        byte[] valueBytes = getValueSerde().serializer().serialize(null, value);
        dbAccessor.put(keyBytes, valueBytes, ((RocksDBTransaction) tx).getTx());
        return originalValue;
    }

    @Override
    public V remove(K key, Transaction tx) {
        Objects.requireNonNull(key, "key cannot be null");
        return put((K) key, null);
    }

    @Override
    public KeyValueIterator<K, V> range(K from, boolean fromInclusive, K to, boolean toInclusive, Transaction tx) {
        return range(from, fromInclusive, to, toInclusive, false, tx);
    }

    private KeyValueIterator<K, V> range(K from, boolean fromInclusive, K to, boolean toInclusive, boolean isDescending, Transaction tx) {
        byte[] fromBytes = getKeySerde().serializer().serialize(null, from);
        byte[] toBytes = getKeySerde().serializer().serialize(null, to);

        validateStoreOpen();

        final KeyValueIterator<byte[], byte[]> rocksDBIterator =
            dbAccessor.range(fromBytes, fromInclusive, toBytes, toInclusive, isDescending, ((RocksDBTransaction) tx).getTx());
        getOpenIterators().add(rocksDBIterator);

        return KeyValueIterators.transformRawIterator(getKeySerde(), getValueSerde(), rocksDBIterator);
    }

    interface RocksDBTransactionAccessor {

        void put(final byte[] key,
                 final byte[] value,
                 org.rocksdb.Transaction tx);

        byte[] get(final byte[] key, org.rocksdb.Transaction tx) throws RocksDBException;

        KeyValueIterator<byte[], byte[]> range(byte[] from, boolean fromInclusive,
                                               byte[] to, boolean toInclusive, boolean isDescending,
                                               org.rocksdb.Transaction tx);
    }

    class SingleColumnFamilyAccessor implements RocksDBTransactionAccessor {
        private final ColumnFamilyHandle columnFamily;

        SingleColumnFamilyAccessor(final ColumnFamilyHandle columnFamily) {
            this.columnFamily = columnFamily;
        }

        @Override
        public void put(final byte[] key,
                        final byte[] value,
                        org.rocksdb.Transaction tx) {
            if (value == null) {
                try {
                    tx.delete(columnFamily, key);
                } catch (final RocksDBException e) {
                    // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new CacheException("Error while removing key from store " + getName(), e);
                }
            } else {
                try {
                    tx.put(columnFamily, key, value);
                } catch (final RocksDBException e) {
                    // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new CacheException("Error while putting key/value into store " + getName(), e);
                }
            }
        }

        @Override
        public byte[] get(final byte[] key, org.rocksdb.Transaction tx) throws RocksDBException {
            return tx.get(columnFamily, new ReadOptions(), key);
        }

        @Override
        public KeyValueIterator<byte[], byte[]> range(byte[] from, boolean fromInclusive,
                                                      byte[] to, boolean toInclusive, boolean isDescending,
                                                      org.rocksdb.Transaction tx) {
            Comparator<byte[]> bytesComparator = new RocksDBKeyComparator<>(getKeySerde(), comparator);

            if (from != null && to != null) {
                int cmp = bytesComparator.compare(from, to);
                if ((isDescending && cmp < 0) || (!isDescending && cmp > 0)) {
                    log.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                        + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                        "Note that the built-in numerical serdes do not follow this for negative numbers");
                    return KeyValueIterators.emptyIterator();
                }
            }

            return new RocksDBRangeIterator(
                getName(),
                tx.getIterator(new ReadOptions(), columnFamily),
                getOpenIterators(),
                from,
                fromInclusive,
                to,
                toInclusive,
                isDescending,
                bytesComparator);
        }
    }
}
