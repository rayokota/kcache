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
import org.apache.kafka.common.utils.Bytes;
import org.rocksdb.RocksIterator;

import java.util.Comparator;
import java.util.Set;

class RocksDBRangeIterator extends RocksDBIterator {
    // RocksDB's JNI interface does not expose getters/setters that allow the
    // comparator to be pluggable, and the default is lexicographic, so it's
    // safe to just force lexicographic comparator here for now.
    private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
    private final byte[] rawFromKey;
    private final boolean fromInclusive;
    private final byte[] rawToKey;
    private final boolean toInclusive;
    private boolean checkAndSkipFrom;

    RocksDBRangeIterator(String storeName,
                         RocksIterator iter,
                         Set<KeyValueIterator<Bytes, byte[]>> openIterators,
                         Bytes from,
                         boolean fromInclusive,
                         Bytes to,
                         boolean toInclusive) {
        super(storeName, iter, openIterators);
        this.rawFromKey = from.get();
        iter.seek(rawFromKey);
        this.fromInclusive = fromInclusive;
        if (fromInclusive) {
            checkAndSkipFrom = true;
        }

        this.rawToKey = to.get();
        if (rawToKey == null) {
            throw new NullPointerException("RocksDBRangeIterator: RawToKey is null for key " + to);
        }
        this.toInclusive = toInclusive;
    }

    @Override
    public KeyValue<Bytes, byte[]> makeNext() {
        KeyValue<Bytes, byte[]> next = super.makeNext();
        if (checkAndSkipFrom) {
            if (next != null && comparator.compare(next.key.get(), rawFromKey) == 0) {
                next = super.makeNext();
            }
            checkAndSkipFrom = false;
        }

        if (next == null) {
            return allDone();
        } else {
            int cmp = comparator.compare(next.key.get(), rawToKey);
            if (cmp < 0 || (cmp == 0 && toInclusive)) {
                return next;
            } else {
                return allDone();
            }
        }
    }
}

