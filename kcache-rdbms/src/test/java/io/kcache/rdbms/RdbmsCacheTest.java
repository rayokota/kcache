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
package io.kcache.rdbms;

import io.kcache.Cache;
import io.kcache.PersistentCacheTest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.junit.After;

public class RdbmsCacheTest extends PersistentCacheTest {

    @Override
    protected Cache<Bytes, byte[]> createCache() throws Exception {
        Cache<Bytes, byte[]> cache =
            new RdbmsCache<>(DB_NAME, dir.getRoot().toString(), Serdes.Bytes(), Serdes.ByteArray());
        Map<String, Object> configs = new HashMap<>();

        //configs.put(RdbmsCache.JDBC_URL_CONFIG, "jdbc:mysql://localhost:3306/kcache");
        //configs.put(RdbmsCache.DIALECT_CONFIG, "MYSQL");
        //configs.put(RdbmsCache.USERNAME_CONFIG, "root");

        //configs.put(RdbmsCache.JDBC_URL_CONFIG, "jdbc:h2:" + dir.newFolder().getAbsolutePath() + "/kcache");
        //configs.put(RdbmsCache.DIALECT_CONFIG, "H2");

        //configs.put(RdbmsCache.JDBC_URL_CONFIG, "jdbc:hsqldb:file:" + dir.newFolder().getAbsolutePath() + "/kcache");
        //configs.put(RdbmsCache.DIALECT_CONFIG, "HSQLDB");

        configs.put(RdbmsCache.JDBC_URL_CONFIG, "jdbc:derby:" + dir.newFolder().getAbsolutePath() + "/kcache;create=true");
        configs.put(RdbmsCache.DIALECT_CONFIG, "DERBY");

        cache.configure(configs);
        return cache;
    }
}
