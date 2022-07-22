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

package io.kcache.rdbms;

import io.kcache.CacheType;
import io.kcache.KafkaCacheConfig;
import io.kcache.KafkaPersistentCacheTest;
import java.util.Properties;

public class KafkaRdbmsCacheTest extends KafkaPersistentCacheTest {

    @Override
    protected Properties getKafkaCacheProperties() throws Exception {
        Properties props = super.getKafkaCacheProperties();
        props.put(KafkaCacheConfig.KAFKACACHE_BACKING_CACHE_CONFIG, CacheType.RDBMS.toString());
        String prefix = KafkaCacheConfig.KAFKACACHE_BACKING_CACHE_CONFIG + "."
            + CacheType.RDBMS + ".";

        //props.put(prefix + RdbmsCache.JDBC_URL_CONFIG, "jdbc:mysql://localhost:3306/kcache");
        //props.put(prefix + RdbmsCache.DIALECT_CONFIG, "MYSQL");
        //props.put(prefix + RdbmsCache.USERNAME_CONFIG, "root");

        //props.put(prefix + RdbmsCache.JDBC_URL_CONFIG, "jdbc:postgresql:postgres");
        //props.put(prefix + RdbmsCache.DIALECT_CONFIG, "POSTGRES");
        //props.put(prefix + RdbmsCache.USERNAME_CONFIG, "postgres");
        //props.put(prefix + RdbmsCache.PASSWORD_CONFIG, "postgres");

        //props.put(prefix + RdbmsCache.JDBC_URL_CONFIG, "jdbc:h2:" + dir.newFolder().getAbsolutePath() + "/kcache");
        //props.put(prefix + RdbmsCache.DIALECT_CONFIG, "H2");

        //props.put(prefix + RdbmsCache.JDBC_URL_CONFIG, "jdbc:hsqldb:file:" + dir.newFolder().getAbsolutePath() + "/kcache");
        //props.put(prefix + RdbmsCache.DIALECT_CONFIG, "HSQLDB");

        props.put(prefix + RdbmsCache.JDBC_URL_CONFIG, "jdbc:derby:" + dir.newFolder().getAbsolutePath() + "/kcache;create=true");
        props.put(prefix + RdbmsCache.DIALECT_CONFIG, "DERBY");

        return props;
    }
}
