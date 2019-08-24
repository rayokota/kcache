/*
 * Copyright 2014-2018 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kcache;

import io.kcache.exceptions.CacheInitializationException;

import java.io.Closeable;
import java.util.Map;

public interface Cache<K, V> extends Map<K, V>, Closeable {

    void init() throws CacheInitializationException;

    KeyValueIterator<K, V> range(final K from, final K to);

    KeyValueIterator<K, V> all();
}
