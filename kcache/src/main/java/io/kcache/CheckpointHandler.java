/*
 * Copyright 2014-2024 Confluent Inc.
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

import io.kcache.exceptions.CacheInitializationException;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;

public interface CheckpointHandler extends Configurable, Closeable {

    /**
     * Configures the checkpoint handler.
     */
    default void configure(Map<String, ?> configs) {
    }

    /**
     * Initializes the checkpoint handler.
     */
    void init() throws CacheInitializationException;

    /**
     * Returns the current checkpoints.
     *
     * @return the current checkpoints
     */
    Map<TopicPartition, Long> checkpoints();

    /**
     * Updates the checkpoints.
     *
     * @param checkpoints the new checkpoints
     */
    void updateCheckpoints(Map<TopicPartition, Long> checkpoints) throws IOException;

    @Override
    default void close() throws IOException {
    }
}
