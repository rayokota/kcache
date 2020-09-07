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

import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.CursorIterable.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;

class LmdbIterator implements KeyValueIterator<byte[], byte[]> {

    private final Txn<ByteBuffer> txn;
    private final CursorIterable<ByteBuffer> iterable;
    private final Iterator<KeyVal<ByteBuffer>> iterator;

    LmdbIterator(final Env<ByteBuffer> env,
                 final Dbi<ByteBuffer> db,
                 final KeyRange<ByteBuffer> keyRange) {
        this.txn = env.txnRead();
        this.iterable = db.iterate(txn, keyRange);
        this.iterator = this.iterable.iterator();
        txn.reset();
    }

    @Override
    public synchronized boolean hasNext() {
        txn.renew();
        try {
            return iterator.hasNext();
        } finally {
            txn.reset();
        }
    }

    @Override
    public KeyValue<byte[], byte[]> next() {
        txn.renew();
        try {
            KeyVal<ByteBuffer> keyVal = iterator.next();
            if (keyVal == null) {
                return null;
            }
            ByteBuffer keyBuf = keyVal.key();
            ByteBuffer valueBuf = keyVal.val();
            byte[] keyBytes = new byte[keyBuf.remaining()];
            byte[] valueBytes = new byte[valueBuf.remaining()];
            keyBuf.get(keyBytes);
            valueBuf.get(valueBytes);
            return new KeyValue<>(keyBytes, valueBytes);
        } finally {
            txn.reset();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("LMDB iterator does not support remove()");
    }

    @Override
    public synchronized void close() {
        iterable.close();
        txn.close();
    }
}
