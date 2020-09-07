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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.kafka.common.serialization.Serde;

import java.util.NoSuchElementException;

public class KeyValueIterators {

    private static final KeyValueIterator EMPTY_ITERATOR = new EmptyKeyValueIterator();

    public static <K, V> KeyValueIterator<K, V> limit(final KeyValueIterator<K, V> iterator, final int limit) {
        return new KeyValueIterator<K, V>() {
            private int count = 0;

            @Override
            public boolean hasNext() {
                return iterator.hasNext() && this.count < limit;
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public KeyValue<K, V> next() {
                if (this.count++ >= limit) {
                    throw new NoSuchElementException();
                }
                return iterator.next();
            }

            @Override
            public void close() {
                iterator.close();
            }
        };
    }

    public static <K, V> KeyValueIterator<K, V> filter(final KeyValueIterator<K, V> iterator, final Predicate<KeyValue<K, V>> predicate) {
        return new KeyValueIterator<K, V>() {
            KeyValue<K, V> nextResult = null;

            @Override
            public boolean hasNext() {
                if (null != this.nextResult) {
                    return true;
                } else {
                    advance();
                    return null != this.nextResult;
                }
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public KeyValue<K, V> next() {
                try {
                    if (null != this.nextResult) {
                        return this.nextResult;
                    } else {
                        advance();
                        if (null != this.nextResult)
                            return this.nextResult;
                        else
                            throw new NoSuchElementException();
                    }
                } finally {
                    this.nextResult = null;
                }
            }

            private void advance() {
                this.nextResult = null;
                while (iterator.hasNext()) {
                    final KeyValue<K, V> s = iterator.next();
                    if (predicate.test(s)) {
                        this.nextResult = s;
                        return;
                    }
                }
            }

            @Override
            public void close() {
                iterator.close();
            }
        };
    }

    public static <K, V, E> Iterator<E> flatMap(final KeyValueIterator<K, V> iterator, final Function<KeyValue<K, V>, Iterator<E>> function) {
        return new Iterator<E>() {

            private Iterator<E> currentIterator = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                if (this.currentIterator.hasNext())
                    return true;
                else {
                    while (iterator.hasNext()) {
                        this.currentIterator = function.apply(iterator.next());
                        if (this.currentIterator.hasNext())
                            return true;
                    }
                }
                return false;
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public E next() {
                if (this.hasNext())
                    return this.currentIterator.next();
                else
                    throw new NoSuchElementException();
            }
        };
    }

    @SafeVarargs
    public static <K, V> KeyValueIterator<K, V> concat(final KeyValueIterator<K, V>... iterators) {
        final MultiIterator<K, V> iterator = new MultiIterator<K, V>();
        for (final KeyValueIterator<K, V> itty : iterators) {
            iterator.addIterator(itty);
        }
        return iterator;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> KeyValueIterator<K, V> emptyIterator() {
        return (KeyValueIterator<K, V>) EMPTY_ITERATOR;
    }

    public static <K, V> KeyValueIterator<K, V> singletonIterator(KeyValue<K, V> keyValue) {
        return new SingletonKeyValueIterator<>(keyValue);
    }

    public static <K, V> KeyValueIterator<K, V> transformRawIterator(
        Serde<K> keySerde, Serde<V> valueSerde, KeyValueIterator<byte[], byte[]> rawIterator) {
        return new TransformedRawKeyValueIterator<>(keySerde, valueSerde, rawIterator);
    }

    private static class MultiIterator<K, V> implements KeyValueIterator<K, V>, Serializable {

        private final List<KeyValueIterator<K, V>> iterators = new ArrayList<>();
        private int current = 0;

        public void addIterator(final KeyValueIterator<K, V> iterator) {
            this.iterators.add(iterator);
        }

        @Override
        public boolean hasNext() {
            if (this.current >= this.iterators.size())
                return false;

            KeyValueIterator<K, V> currentIterator = this.iterators.get(this.current);

            while (true) {
                if (currentIterator.hasNext()) {
                    return true;
                } else {
                    this.current++;
                    if (this.current >= iterators.size())
                        break;
                    currentIterator = iterators.get(this.current);
                }
            }
            return false;
        }

        @Override
        public void remove() {
            this.iterators.get(this.current).remove();
        }

        @Override
        public KeyValue<K, V> next() {
            if (this.iterators.isEmpty()) throw new NoSuchElementException();

            KeyValueIterator<K, V> currentIterator = iterators.get(this.current);
            while (true) {
                if (currentIterator.hasNext()) {
                    return currentIterator.next();
                } else {
                    this.current++;
                    if (this.current >= iterators.size())
                        break;
                    currentIterator = iterators.get(current);
                }
            }
            throw new NoSuchElementException();
        }

        public void clear() {
            this.iterators.clear();
            this.current = 0;
        }

        @Override
        public void close() {
            this.iterators.forEach(KeyValueIterator::close);
        }
    }

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
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static class SingletonKeyValueIterator<K, V> implements KeyValueIterator<K, V> {
        private boolean beforeFirst;
        private KeyValue<K, V> keyValue;

        public SingletonKeyValueIterator(KeyValue<K, V> keyValue) {
            this.beforeFirst = true;
            this.keyValue = keyValue;
        }

        @Override
        public boolean hasNext() {
            return this.beforeFirst;
        }

        @Override
        public KeyValue<K, V> next() {
            if (this.beforeFirst) {
                this.beforeFirst = false;
                return this.keyValue;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
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

        @Override
        public final boolean hasNext() {
            return this.rawIterator.hasNext();
        }

        @Override
        public final KeyValue<K, V> next() {
            KeyValue<byte[], byte[]> keyValue = this.rawIterator.next();
            return new KeyValue<>(
                keySerde.deserializer().deserialize(null, keyValue.key),
                valueSerde.deserializer().deserialize(null, keyValue.value)
            );
        }

        @Override
        public final void remove() {
            this.rawIterator.remove();
        }

        @Override
        public final void close() {
            this.rawIterator.close();
        }
    }
}
