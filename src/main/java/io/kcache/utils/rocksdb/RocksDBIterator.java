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

import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import io.kcache.exceptions.CacheException;
import org.apache.kafka.common.utils.AbstractIterator;
import org.rocksdb.RocksIterator;

import java.util.Set;

class RocksDBIterator extends AbstractIterator<KeyValue<byte[], byte[]>> implements KeyValueIterator<byte[], byte[]> {

    private final String storeName;
    private final RocksIterator iter;
    private final Set<KeyValueIterator<byte[], byte[]>> openIterators;
    private final boolean isDescending;

    private volatile boolean open = true;

    private KeyValue<byte[], byte[]> next;

    RocksDBIterator(final String storeName,
                    final RocksIterator iter,
                    final Set<KeyValueIterator<byte[], byte[]>> openIterators,
                    final boolean isDescending) {
        this.storeName = storeName;
        this.iter = iter;
        this.openIterators = openIterators;
        this.isDescending = isDescending;
        if (isDescending) {
            iter.seekToLast();
        } else {
            iter.seekToFirst();
        }
    }

    @Override
    public synchronized boolean hasNext() {
        if (!open) {
            throw new CacheException(String.format("RocksDB iterator for store %s has closed", storeName));
        }
        return super.hasNext();
    }

    @Override
    public KeyValue<byte[], byte[]> makeNext() {
        if (!iter.isValid()) {
            return allDone();
        } else {
            next = getKeyValue();
            if (isDescending) {
                iter.prev();
            } else {
                iter.next();
            }
            return next;
        }
    }

    private KeyValue<byte[], byte[]> getKeyValue() {
        return new KeyValue<>(iter.key(), iter.value());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("RocksDB iterator does not support remove()");
    }

    @Override
    public synchronized void close() {
        openIterators.remove(this);
        iter.close();
        open = false;
    }
}
