/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kcache.rocksdb;

import io.kcache.utils.KeyBytesComparator;
import java.nio.ByteBuffer;
import org.apache.kafka.common.serialization.Serde;
import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;

import java.util.Comparator;

public class RocksDBKeySliceComparator<K> extends AbstractComparator {

    // Ensure ComparatorOptions is assigned to a variable
    // See https://github.com/facebook/rocksdb/issues/6608
    private final static ComparatorOptions OPTIONS = new ComparatorOptions();
    private final Comparator<byte[]> comparator;

    public RocksDBKeySliceComparator(Serde<K> keySerde, Comparator<? super K> keyComparator) {
        super(OPTIONS);
        this.comparator = new KeyBytesComparator<>(keySerde, keyComparator);
    }

    @Override
    public String name() {
        return getClass().getName();
    }

    @Override
    public int compare(ByteBuffer buf1, ByteBuffer buf2) {
        byte[] arr1 = new byte[buf1.remaining()];
        buf1.get(arr1);
        byte[] arr2 = new byte[buf2.remaining()];
        buf2.get(arr2);
        return comparator.compare(arr1, arr2);
    }
}
