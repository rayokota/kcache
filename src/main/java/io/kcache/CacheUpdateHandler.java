/*
 * Copyright 2014-2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kcache;

import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.io.IOException;

public interface CacheUpdateHandler<K, V> extends Closeable {

    /**
     * Invoked before every new K,V pair written to the cache
     *
     * @param key   key associated with the data
     * @param value data written to the cache
     * @param tp topic-partition
     * @param offset offset
     * @param timestamp timestamp
     * @return whether the update should proceed
     */
    default boolean validateUpdate(K key, V value, TopicPartition tp, long offset, long timestamp) {
        return true;
    }

    /**
     * Invoked after every new K,V pair written to the cache
     *
     * @param key   key associated with the data
     * @param value data written to the cache
     * @param oldValue the previous value associated with key, or null if there was no mapping for key
     * @param tp topic-partition
     * @param offset offset
     * @param timestamp timestamp
     */
    void handleUpdate(K key, V value, V oldValue, TopicPartition tp, long offset, long timestamp);

    /**
     * Invoked after a batch of updates.
     *
     * @param count batch count
     */
    default void checkpoint(int count) {
    }

    @Override
    default void close() throws IOException {
    }
}
