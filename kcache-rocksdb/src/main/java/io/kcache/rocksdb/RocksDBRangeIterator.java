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
package io.kcache.rocksdb;

import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import org.rocksdb.RocksIterator;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

class RocksDBRangeIterator extends RocksDBIterator {
    private final byte[] rawFromKey;
    private final boolean fromInclusive;
    private final byte[] rawToKey;
    private final boolean toInclusive;
    private final Comparator<byte[]> comparator;
    private boolean checkAndSkipFrom;

    RocksDBRangeIterator(String storeName,
                         RocksIterator iter,
                         Set<KeyValueIterator<byte[], byte[]>> openIterators,
                         byte[] from,
                         boolean fromInclusive,
                         byte[] to,
                         boolean toInclusive,
                         boolean isDescending,
                         Comparator<byte[]> comparator) {
        super(storeName, iter, openIterators, isDescending);
        this.rawFromKey = from;
        if (rawFromKey == null) {
            if (isDescending) {
                iter.seekToLast();
            } else {
                iter.seekToFirst();
            }
        } else {
            if (isDescending) {
                iter.seekForPrev(rawFromKey);
            } else {
                iter.seek(rawFromKey);
            }
        }
        this.fromInclusive = fromInclusive;
        if (rawFromKey != null && !fromInclusive) {
            checkAndSkipFrom = true;
        }

        this.rawToKey = to;
        this.toInclusive = toInclusive;
        this.comparator = isDescending ? Collections.reverseOrder(comparator) : comparator;
    }

    @Override
    public KeyValue<byte[], byte[]> makeNext() {
        KeyValue<byte[], byte[]> next = super.makeNext();
        if (checkAndSkipFrom) {
            if (next != null && comparator.compare(next.key, rawFromKey) == 0) {
                next = super.makeNext();
            }
            checkAndSkipFrom = false;
        }

        if (next == null) {
            return allDone();
        } else if (rawToKey == null) {
            return next;
        } else {
            int cmp = comparator.compare(next.key, rawToKey);
            if (cmp < 0 || (cmp == 0 && toInclusive)) {
                return next;
            } else {
                return allDone();
            }
        }
    }
}

