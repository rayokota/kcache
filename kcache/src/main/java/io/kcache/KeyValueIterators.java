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

package io.kcache;

import org.apache.kafka.common.serialization.Serde;

import java.util.NoSuchElementException;

public class KeyValueIterators {

    private static class EmptyKeyValueIterator<K, V> implements KeyValueIterator<K, V> {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public KeyValue<K, V> next() {
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
        }

        @Override
        public void close() {
        }
    }

    private static final KeyValueIterator EMPTY_ITERATOR = new EmptyKeyValueIterator();

    @SuppressWarnings("unchecked")
    public static <K, V> KeyValueIterator<K, V> emptyIterator() {
        return (KeyValueIterator<K, V>) EMPTY_ITERATOR;
    }

    private static class TransformedRawKeyValueIterator<K, V> implements KeyValueIterator<K, V> {
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;
        private final KeyValueIterator<byte[], byte[]> rawIterator;

        TransformedRawKeyValueIterator(
            Serde<K> keySerde, Serde<V> valueSerde, KeyValueIterator<byte[], byte[]> rawIterator) {
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.rawIterator = rawIterator;
        }

        public final boolean hasNext() {
            return this.rawIterator.hasNext();
        }

        public final KeyValue<K, V> next() {
            KeyValue<byte[], byte[]> keyValue = this.rawIterator.next();
            return new KeyValue<>(
                keySerde.deserializer().deserialize(null, keyValue.key),
                valueSerde.deserializer().deserialize(null, keyValue.value)
            );
        }

        public final void remove() {
            this.rawIterator.remove();
        }

        public final void close() {
            this.rawIterator.close();
        }
    }

    public static <K, V> KeyValueIterator<K, V> transformRawIterator(
        Serde<K> keySerde, Serde<V> valueSerde, KeyValueIterator<byte[], byte[]> rawIterator) {
        return new TransformedRawKeyValueIterator<>(keySerde, valueSerde, rawIterator);
    }
}
