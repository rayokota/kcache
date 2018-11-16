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

import io.kcache.CacheUpdateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringUpdateHandler implements CacheUpdateHandler<String, String> {

    private static final Logger log = LoggerFactory.getLogger(StringUpdateHandler.class);

    /**
     * Invoked on every new K,V pair written to the store
     *
     * @param key   key associated with the data
     * @param value data written to the store
     */
    @Override
    public void handleUpdate(String key, String value) {
        log.info("Handle update for ({}, {})", key, value);
    }
}
