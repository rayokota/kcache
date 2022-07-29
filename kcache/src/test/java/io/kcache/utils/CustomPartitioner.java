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

package io.kcache.utils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class CustomPartitioner implements Partitioner {

    public static final Set<Object> keys = ConcurrentHashMap.newKeySet();
    public static final Set<Object> values = ConcurrentHashMap.newKeySet();

    public void configure(Map<String, ?> configs) {
    }

    public int partition(String topic, Object key, byte[] keyBytes,
        Object value, byte[] valueBytes, Cluster cluster) {
        keys.add(key);
        values.add(value);
        return 0;
    }

    public void close() {
    }
}
