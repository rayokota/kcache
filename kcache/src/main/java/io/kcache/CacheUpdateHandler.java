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

import java.util.Map;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.io.IOException;

public interface CacheUpdateHandler<K, V> extends Closeable {

    enum ValidationStatus {
        SUCCESS,
        ROLLBACK_FAILURE,
        IGNORE_FAILURE
    }

    /**
     * Invoked after the cache is initialized.
     *
     * @param checkpoints current checkpoints
     */
    default void cacheInitialized(Map<TopicPartition, Long> checkpoints) {
    }

    /**
     * Invoked before a batch of updates.
     * @param count batch count
     */
    default void startBatch(int count) {
    }

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
    default ValidationStatus validateUpdate(K key, V value, TopicPartition tp, long offset, long timestamp) {
        return ValidationStatus.SUCCESS;
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
     * Retrieve the offsets to checkpoint.
     *
     * @param count batch count
     * @return the offsets to checkpoint, or null
     */
    default Map<TopicPartition, Long> checkpoint(int count) {
        return null;
    }

    /**
     * Invoked after a batch of updates.
     *
     * @param count batch count
     */
    default void endBatch(int count) {
    }

    @Override
    default void close() throws IOException {
    }
}
