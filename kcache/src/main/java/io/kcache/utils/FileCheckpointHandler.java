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

package io.kcache.utils;

import io.kcache.CheckpointHandler;
import io.kcache.KafkaCacheConfig;
import io.kcache.exceptions.CacheInitializationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileCheckpointHandler implements CheckpointHandler {

    private static final Logger log = LoggerFactory.getLogger(FileCheckpointHandler.class);

    private String topic;
    private String checkpointDir;
    private int checkpointVersion;
    private OffsetCheckpoint checkpointFile;
    private final Map<TopicPartition, Long> checkpointFileCache = new HashMap<>();

    @Override
    public void configure(Map<String, ?> configs) {
        KafkaCacheConfig config = new KafkaCacheConfig(configs);
        this.topic = config.getString(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG);
        this.checkpointDir = config.getString(KafkaCacheConfig.KAFKACACHE_CHECKPOINT_DIR_CONFIG);
        this.checkpointVersion = config.getInt(KafkaCacheConfig.KAFKACACHE_CHECKPOINT_VERSION_CONFIG);
    }

    @Override
    public void init() throws CacheInitializationException {
        try {
            checkpointFile = new OffsetCheckpoint(checkpointDir, checkpointVersion, topic);
            checkpointFileCache.putAll(checkpointFile.read());
        } catch (IOException e) {
            throw new CacheInitializationException("Failed to read checkpoints", e);
        }
    }

    @Override
    public Map<TopicPartition, Long> checkpoints() {
        return new HashMap<>(checkpointFileCache);
    }

    @Override
    public void updateCheckpoints(Map<TopicPartition, Long> checkpoints) throws IOException {
        checkpointFileCache.putAll(checkpoints);
        checkpointFile.write(checkpointFileCache);
    }

    @Override
    public void close() throws IOException {
        if (checkpointFile != null) {
            checkpointFile.close();
        }
    }
}
