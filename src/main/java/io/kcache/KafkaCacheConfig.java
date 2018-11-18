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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class KafkaCacheConfig extends AbstractConfig {

    /**
     * <code>kafkacache.bootstrap.servers</code>
     */
    public static final String KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG = "kafkacache.bootstrap.servers";
    /**
     * <code>kafkacache.group.id</code>
     */
    public static final String KAFKACACHE_GROUP_ID_CONFIG = "kafkacache.group.id";
    public static final String DEFAULT_KAFKACACHE_GROUP_ID = "kafkacache";
    /**
     * <code>kafkacache.topic</code>
     */
    public static final String KAFKACACHE_TOPIC_CONFIG = "kafkacache.topic";
    public static final String DEFAULT_KAFKACACHE_TOPIC = "_cache";
    /**
     * <code>kafkacache.topic.replication.factor</code>
     */
    public static final String KAFKACACHE_TOPIC_REPLICATION_FACTOR_CONFIG = "kafkacache.topic.replication.factor";
    public static final int DEFAULT_KAFKACACHE_TOPIC_REPLICATION_FACTOR = 3;
    /**
     * <code>kafkacache.timeout.ms</code>
     */
    public static final String KAFKACACHE_TIMEOUT_CONFIG = "kafkacache.timeout.ms";
    /**
     * <code>kafkacache.init.timeout.ms</code>
     */
    public static final String KAFKACACHE_INIT_TIMEOUT_CONFIG = "kafkacache.init.timeout.ms";

    protected static final String KAFKACACHE_BOOTSTRAP_SERVERS_DOC =
        "A list of Kafka brokers to connect to. For example, `PLAINTEXT://hostname:9092,SSL://hostname2:9092`.";
    protected static final String KAFKACACHE_GROUP_ID_DOC =
        "Use this setting to override the group.id for the Kafka cache consumer.";
    protected static final String KAFKACACHE_TOPIC_DOC =
        "The durable single partition topic that acts as the durable log for the data.";
    protected static final String KAFKACACHE_TOPIC_REPLICATION_FACTOR_DOC =
        "The desired replication factor of the topic. The actual replication factor "
            + "will be the smaller of this value and the number of live Kafka brokers.";
    protected static final String KAFKACACHE_INIT_TIMEOUT_DOC =
        "The timeout for initialization of the Kafka cache, including creation of the Kafka topic "
            + "that stores data.";
    protected static final String KAFKACACHE_TIMEOUT_DOC =
        "The timeout for an operation on the Kafka cache.";

    private static final ConfigDef config;

    static {
        config = baseConfigDef();
    }

    public static ConfigDef baseConfigDef() {

        return new ConfigDef()
            .define(KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, "",
                ConfigDef.Importance.HIGH, KAFKACACHE_BOOTSTRAP_SERVERS_DOC
            )
            .define(KAFKACACHE_TOPIC_CONFIG, ConfigDef.Type.STRING, DEFAULT_KAFKACACHE_TOPIC,
                ConfigDef.Importance.HIGH, KAFKACACHE_TOPIC_DOC
            )
            .define(KAFKACACHE_TOPIC_REPLICATION_FACTOR_CONFIG, ConfigDef.Type.INT,
                DEFAULT_KAFKACACHE_TOPIC_REPLICATION_FACTOR,
                ConfigDef.Importance.HIGH, KAFKACACHE_TOPIC_REPLICATION_FACTOR_DOC
            )
            .define(KAFKACACHE_INIT_TIMEOUT_CONFIG, ConfigDef.Type.INT, 60000, atLeast(0),
                ConfigDef.Importance.MEDIUM, KAFKACACHE_INIT_TIMEOUT_DOC
            )
            .define(KAFKACACHE_TIMEOUT_CONFIG, ConfigDef.Type.INT, 500, atLeast(0),
                ConfigDef.Importance.MEDIUM, KAFKACACHE_TIMEOUT_DOC
            )
            .define(KAFKACACHE_GROUP_ID_CONFIG, ConfigDef.Type.STRING, DEFAULT_KAFKACACHE_GROUP_ID,
                ConfigDef.Importance.LOW, KAFKACACHE_GROUP_ID_DOC
            );
    }

    public KafkaCacheConfig(Properties props) {
        this(config, props);
    }

    public KafkaCacheConfig(ConfigDef configDef, Properties props) {
        super(configDef, props);
    }

    public String bootstrapBrokers() {
        return String.join(",", getList(KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG));
    }
}
