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
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.io.IOException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

public interface CacheUpdateHandler<K, V> extends Closeable {

    enum ValidationStatus {
        SUCCESS,
        ROLLBACK_FAILURE,
        IGNORE_FAILURE
    }

    /**
     * Invoked after the cache is initialized.
     *
     * @param count the number of records consumed by the invoking thread
     * @param checkpoints current checkpoints
     */
    default void cacheInitialized(int count, Map<TopicPartition, Long> checkpoints) {
    }

    /**
     * Invoked after the cache is reset.
     */
    default void cacheReset() {
    }

    /**
     * Invoked after the cache is synchronized.
     *
     * @param count the number of records consumed by the invoking thread
     * @param checkpoints current checkpoints
     */
    default void cacheSynchronized(int count, Map<TopicPartition, Long> checkpoints) {
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
     * @param ts timestamp
     * @return whether the update should proceed
     */
    default ValidationStatus validateUpdate(K key, V value, TopicPartition tp, long offset, long ts) {
        return ValidationStatus.SUCCESS;
    }

    /**
     * Invoked before every new K,V pair written to the cache
     *
     * @param headers headers
     * @param key   key associated with the data
     * @param value data written to the cache
     * @param tp topic-partition
     * @param offset offset
     * @param ts timestamp
     * @param tsType timestamp type
     * @param leaderEpoch leader epoch
     * @return whether the update should proceed
     */
    default ValidationStatus validateUpdate(Headers headers, K key, V value, TopicPartition tp,
                                            long offset, long ts, TimestampType tsType,
                                            Optional<Integer> leaderEpoch) {
        return validateUpdate(key, value, tp, offset, ts);
    }

    /**
     * Invoked after every new K,V pair written to the cache
     *
     * @param key   key associated with the data
     * @param value data written to the cache
     * @param oldValue the previous value associated with key, or null if there was no mapping for key
     * @param tp topic-partition
     * @param offset offset
     * @param ts timestamp
     */
    void handleUpdate(K key, V value, V oldValue, TopicPartition tp, long offset, long ts);

    /**
     * Invoked after every new K,V pair written to the cache
     *
     * @param headers headers
     * @param key   key associated with the data
     * @param value data written to the cache
     * @param oldValue the previous value associated with key, or null if there was no mapping for key
     * @param tp topic-partition
     * @param offset offset
     * @param ts timestamp
     * @param tsType timestamp type
     * @param leaderEpoch leader epoch
     */
    default void handleUpdate(Headers headers, K key, V value, V oldValue, TopicPartition tp,
                              long offset, long ts, TimestampType tsType,
                              Optional<Integer> leaderEpoch) {
        handleUpdate(key, value, oldValue, tp, offset, ts);
    }

    /**
     * Retrieve the offsets to checkpoint.
     *
     * @param count batch count
     * @return the offsets to checkpoint, or null to use the latest offsets
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

    /**
     * Invoked when an error has occurred while processing the batch.
     *
     * @param count batch count
     * @param t the error
     */
    default void failBatch(int count, Throwable t) {
    }

    /**
     * Invoked after the cache is flushed.
     */
    default void cacheFlushed() {
    }

    @Override
    default void close() throws IOException {
    }

    /**
     * Invoked after the cache is destroyed.
     */
    default void cacheDestroyed() {
    }
}
