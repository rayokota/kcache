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

import io.kcache.utils.EnumRecommender;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public class KafkaCacheConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaCacheConfig.class);

    /**
     * <code>kafkacache.bootstrap.servers</code>
     */
    public static final String KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG = "kafkacache.bootstrap.servers";
    /**
     * <code>kafkacache.group.id</code>
     */
    public static final String KAFKACACHE_GROUP_ID_CONFIG = "kafkacache.group.id";
    public static final String DEFAULT_KAFKACACHE_GROUP_ID_PREFIX = "kafka-cache";
    /**
     * <code>kafkacache.client.id</code>
     */
    public static final String KAFKACACHE_CLIENT_ID_CONFIG = "kafkacache.client.id";
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
     * <code>kafkacache.topic.num.partitions</code>
     */
    public static final String KAFKACACHE_TOPIC_NUM_PARTITIONS_CONFIG = "kafkacache.topic.num.partitions";
    public static final int DEFAULT_KAFKACACHE_TOPIC_NUM_PARTITIONS = 1;
    /**
     * <code>kafkacache.topic.partitions</code>
     */
    public static final String KAFKACACHE_TOPIC_PARTITIONS_CONFIG = "kafkacache.topic.partitions";
    /**
     * <code>kafkacache.topic.partitions.offset</code>
     */
    public static final String KAFKACACHE_TOPIC_PARTITIONS_OFFSET_CONFIG = "kafkacache.topic.partitions.offset";
    public static final String DEFAULT_KAFKACACHE_TOPIC_PARTITIONS_OFFSET = OffsetType.BEGINNING.toString();
    /**
     * <code>kafkacache.topic.skip.validation</code>
     */
    public static final String KAFKACACHE_TOPIC_SKIP_VALIDATION_CONFIG = "kafkacache.topic.skip.validation";
    /**
     * <code>kafkacache.topic.require.compact</code>
     */
    public static final String KAFKACACHE_TOPIC_REQUIRE_COMPACT_CONFIG = "kafkacache.topic.require.compact";
    /**
     * <code>kafkacache.topic.read.only</code>
     */
    public static final String KAFKACACHE_TOPIC_READ_ONLY_CONFIG = "kafkacache.topic.read.only";
    /**
     * <code>kafkacache.timeout.ms</code>
     */
    public static final String KAFKACACHE_TIMEOUT_CONFIG = "kafkacache.timeout.ms";
    /**
     * <code>kafkacache.init.timeout.ms</code>
     */
    public static final String KAFKACACHE_INIT_TIMEOUT_CONFIG = "kafkacache.init.timeout.ms";
    /**
     * <code>kafkacache.backing.cache</code>
     */
    public static final String KAFKACACHE_BACKING_CACHE_CONFIG = "kafkacache.backing.cache";
    /**
     * <code>kafkacache.bounded.cache.size</code>
     */
    public static final String KAFKACACHE_BOUNDED_CACHE_SIZE_CONFIG = "kafkacache.bounded.cache.size";
    /**
     * <code>kafkacache.bounded.cache.expiry.secs</code>
     */
    public static final String KAFKACACHE_BOUNDED_CACHE_EXPIRY_SECS_CONFIG = "kafkacache.bounded.cache.expiry.secs";
    /**
     * <code>kafkacache.checkpoint.dir</code>
     */
    public static final String KAFKACACHE_CHECKPOINT_DIR_CONFIG = "kafkacache.checkpoint.dir";
    /**
     * <code>kafkacache.checkpoint.version</code>
     */
    public static final String KAFKACACHE_CHECKPOINT_VERSION_CONFIG = "kafkacache.checkpoint.version";
    /**
     * <code>kafkacache.data.dir</code>
     */
    public static final String KAFKACACHE_DATA_DIR_CONFIG = "kafkacache.data.dir";

    public static final String KAFKACACHE_SECURITY_PROTOCOL_CONFIG =
        "kafkacache.security.protocol";
    public static final String KAFKACACHE_SSL_TRUSTSTORE_LOCATION_CONFIG =
        "kafkacache.ssl.truststore.location";
    public static final String KAFKACACHE_SSL_TRUSTSTORE_PASSWORD_CONFIG =
        "kafkacache.ssl.truststore.password";
    public static final String KAFKACACHE_SSL_KEYSTORE_LOCATION_CONFIG =
        "kafkacache.ssl.keystore.location";
    public static final String KAFKACACHE_SSL_TRUSTSTORE_TYPE_CONFIG =
        "kafkacache.ssl.truststore.type";
    public static final String KAFKACACHE_SSL_TRUSTMANAGER_ALGORITHM_CONFIG =
        "kafkacache.ssl.trustmanager.algorithm";
    public static final String KAFKACACHE_SSL_KEYSTORE_PASSWORD_CONFIG =
        "kafkacache.ssl.keystore.password";
    public static final String KAFKACACHE_SSL_KEYSTORE_TYPE_CONFIG =
        "kafkacache.ssl.keystore.type";
    public static final String KAFKACACHE_SSL_KEYMANAGER_ALGORITHM_CONFIG =
        "kafkacache.ssl.keymanager.algorithm";
    public static final String KAFKACACHE_SSL_KEY_PASSWORD_CONFIG =
        "kafkacache.ssl.key.password";
    public static final String KAFKACACHE_SSL_ENABLED_PROTOCOLS_CONFIG =
        "kafkacache.ssl.enabled.protocols";
    public static final String KAFKACACHE_SSL_PROTOCOL_CONFIG =
        "kafkacache.ssl.protocol";
    public static final String KAFKACACHE_SSL_PROVIDER_CONFIG =
        "kafkacache.ssl.provider";
    public static final String KAFKACACHE_SSL_CIPHER_SUITES_CONFIG =
        "kafkacache.ssl.cipher.suites";
    public static final String KAFKACACHE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG =
        "kafkacache.ssl.endpoint.identification.algorithm";
    public static final String KAFKACACHE_SASL_KERBEROS_SERVICE_NAME_CONFIG =
        "kafkacache.sasl.kerberos.service.name";
    public static final String KAFKACACHE_SASL_MECHANISM_CONFIG =
        "kafkacache.sasl.mechanism";
    public static final String KAFKACACHE_SASL_JAAS_CONFIG_CONFIG =
        "kafkacache.sasl.jaas.config";
    public static final String KAFKACACHE_SASL_KERBEROS_KINIT_CMD_CONFIG =
        "kafkacache.sasl.kerberos.kinit.cmd";
    public static final String KAFKACACHE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_CONFIG =
        "kafkacache.sasl.kerberos.min.time.before.relogin";
    public static final String KAFKACACHE_SASL_KERBEROS_TICKET_RENEW_JITTER_CONFIG =
        "kafkacache.sasl.kerberos.ticket.renew.jitter";
    public static final String KAFKACACHE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_CONFIG =
        "kafkacache.sasl.kerberos.ticket.renew.window.factor";


    protected static final String KAFKACACHE_BOOTSTRAP_SERVERS_DOC =
        "A list of Kafka brokers to connect to. For example, `PLAINTEXT://hostname:9092,SSL://hostname2:9092`.";
    protected static final String KAFKACACHE_GROUP_ID_DOC =
        "Use this setting to override the group.id for the Kafka cache consumer. "
            + "The default is \"kafka-cache-<host>\"";
    protected static final String KAFKACACHE_CLIENT_ID_DOC =
        "Use this setting to override the client.id for the Kafka cache consumer.";
    protected static final String KAFKACACHE_TOPIC_DOC =
        "The durable single partition topic that acts as the durable log for the data.";
    protected static final String KAFKACACHE_TOPIC_REPLICATION_FACTOR_DOC =
        "The desired replication factor of the topic. The actual replication factor "
            + "will be the smaller of this value and the number of live Kafka brokers.";
    protected static final String KAFKACACHE_TOPIC_NUM_PARTITIONS_DOC =
        "The desired number of partitions factor for the topic.";
    protected static final String KAFKACACHE_TOPIC_PARTITIONS_DOC =
        "A list of partitions to consume, or all partitions if not specified.";
    protected static final String KAFKACACHE_TOPIC_PARTITIONS_OFFSET_DOC =
        "The offset to start consuming all partitions from, one of \"beginning\" (the default), "
            + "\"end\", a positive number representing an absolute offset, "
            + "a negative number representing a relative offset from the end, "
            + "or \"@<value>\", where \"<value>\" is a timestamp in ms.";
    protected static final String KAFKACACHE_TOPIC_SKIP_VALIDATION_DOC =
        "Whether to skip topic validation.";
    protected static final String KAFKACACHE_TOPIC_REQUIRE_COMPACT_DOC =
        "Whether to require that the topic is compacted.";
    protected static final String KAFKACACHE_TOPIC_READ_ONLY_DOC =
        "Whether the topic is only used for reading, and thus no writes are allowed.";
    protected static final String KAFKACACHE_INIT_TIMEOUT_DOC =
        "The timeout for initialization of the Kafka cache, including creation of the Kafka topic "
            + "that stores data.";
    protected static final String KAFKACACHE_TIMEOUT_DOC =
        "The timeout for an operation on the Kafka cache.";
    protected static final String KAFKACACHE_BACKING_CACHE_DOC =
        "The type of backing cache, one of `memory`, `bdbje`, `lmdb`, `rdbms`, and `rocksdb`.";
    protected static final String KAFKACACHE_BOUNDED_CACHE_SIZE_DOC =
        "For an in-memory cache, the maximum size of the cache.";
    protected static final String KAFKACACHE_BOUNDED_CACHE_EXPIRY_SECS_DOC =
        "For an in-memory cache, the expiration in seconds for entries added to the cache.";
    protected static final String KAFKACACHE_CHECKPOINT_DIR_DOC =
        "For persistent backing caches, the directory in which to store offset checkpoints.";
    protected static final String KAFKACACHE_CHECKPOINT_VERSION_DOC =
        "For persistent backing caches, the version of the checkpoint offset file.";
    protected static final String KAFKACACHE_DATA_DIR_DOC =
        "For persistent backing caches, the directory in which to store data.";

    protected static final String KAFKACACHE_SECURITY_PROTOCOL_DOC =
        "The security protocol to use when connecting with Kafka, the underlying persistent storage. "
            + "Values can be `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`.";
    protected static final String KAFKACACHE_SSL_TRUSTSTORE_LOCATION_DOC =
        "The location of the SSL trust cache file.";
    protected static final String KAFKACACHE_SSL_TRUSTSTORE_PASSWORD_DOC =
        "The password to access the trust cache.";
    protected static final String KAFAKCACHE_SSL_TRUSTSTORE_TYPE_DOC =
        "The file format of the trust cache.";
    protected static final String KAFKACACHE_SSL_TRUSTMANAGER_ALGORITHM_DOC =
        "The algorithm used by the trust manager factory for SSL connections.";
    protected static final String KAFKACACHE_SSL_KEYSTORE_LOCATION_DOC =
        "The location of the SSL keystore file.";
    protected static final String KAFKACACHE_SSL_KEYSTORE_PASSWORD_DOC =
        "The password to access the keystore.";
    protected static final String KAFAKCACHE_SSL_KEYSTORE_TYPE_DOC =
        "The file format of the keystore.";
    protected static final String KAFKACACHE_SSL_KEYMANAGER_ALGORITHM_DOC =
        "The algorithm used by key manager factory for SSL connections.";
    protected static final String KAFKACACHE_SSL_KEY_PASSWORD_DOC =
        "The password of the key contained in the keystore.";
    protected static final String KAFAKCACHE_SSL_ENABLED_PROTOCOLS_DOC =
        "Protocols enabled for SSL connections.";
    protected static final String KAFAKCACHE_SSL_PROTOCOL_DOC =
        "The SSL protocol used.";
    protected static final String KAFAKCACHE_SSL_PROVIDER_DOC =
        "The name of the security provider used for SSL.";
    protected static final String KAFKACACHE_SSL_CIPHER_SUITES_DOC =
        "A list of cipher suites used for SSL.";
    protected static final String KAFKACACHE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC =
        "The endpoint identification algorithm to validate the server hostname using the server "
            + "certificate.";
    protected static final String KAFKACACHE_SASL_KERBEROS_SERVICE_NAME_DOC =
        "The Kerberos principal name that the Kafka client runs as. This can be defined either in "
            + "the JAAS "
            + "config file or here.";
    protected static final String KAFKACACHE_SASL_MECHANISM_DOC =
        "The SASL mechanism used for Kafka connections. GSSAPI is the default.";
    protected static final String KAFKACACHE_SASL_JAAS_CONFIG_DOC =
        "The JAAS login context parameters for SASL connections.";
    protected static final String KAFKACACHE_SASL_KERBEROS_KINIT_CMD_DOC =
        "The Kerberos kinit command path.";
    protected static final String KAFKACACHE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC =
        "The login time between refresh attempts.";
    protected static final String KAFKACACHE_SASL_KERBEROS_TICKET_RENEW_JITTER_DOC =
        "The percentage of random jitter added to the renewal time.";
    protected static final String KAFKACACHE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC =
        "Login thread will sleep until the specified window factor of time from last refresh to "
            + "ticket's expiry has "
            + "been reached, at which time it will try to renew the ticket.";

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
            .define(KAFKACACHE_TOPIC_NUM_PARTITIONS_CONFIG, ConfigDef.Type.INT,
                DEFAULT_KAFKACACHE_TOPIC_NUM_PARTITIONS,
                ConfigDef.Importance.MEDIUM, KAFKACACHE_TOPIC_NUM_PARTITIONS_DOC
            )
            .define(KAFKACACHE_TOPIC_PARTITIONS_CONFIG, ConfigDef.Type.LIST, "",
                ConfigDef.Importance.MEDIUM, KAFKACACHE_TOPIC_PARTITIONS_DOC
            )
            .define(KAFKACACHE_TOPIC_PARTITIONS_OFFSET_CONFIG, ConfigDef.Type.STRING,
                DEFAULT_KAFKACACHE_TOPIC_PARTITIONS_OFFSET,
                ConfigDef.Importance.MEDIUM, KAFKACACHE_TOPIC_PARTITIONS_OFFSET_DOC
            )
            .define(KAFKACACHE_TOPIC_SKIP_VALIDATION_CONFIG, ConfigDef.Type.BOOLEAN, false,
                ConfigDef.Importance.MEDIUM, KAFKACACHE_TOPIC_SKIP_VALIDATION_DOC
            )
            .define(KAFKACACHE_TOPIC_REQUIRE_COMPACT_CONFIG, ConfigDef.Type.BOOLEAN, true,
                ConfigDef.Importance.MEDIUM, KAFKACACHE_TOPIC_REQUIRE_COMPACT_DOC
            )
            .define(KAFKACACHE_TOPIC_READ_ONLY_CONFIG, ConfigDef.Type.BOOLEAN, false,
                ConfigDef.Importance.MEDIUM, KAFKACACHE_TOPIC_READ_ONLY_DOC
            )
            .define(KAFKACACHE_INIT_TIMEOUT_CONFIG, ConfigDef.Type.INT, 300000, atLeast(0),
                ConfigDef.Importance.MEDIUM, KAFKACACHE_INIT_TIMEOUT_DOC
            )
            .define(KAFKACACHE_TIMEOUT_CONFIG, ConfigDef.Type.INT, 60000, atLeast(0),
                ConfigDef.Importance.MEDIUM, KAFKACACHE_TIMEOUT_DOC
            )
            .define(KAFKACACHE_BACKING_CACHE_CONFIG, ConfigDef.Type.STRING,
                CacheType.MEMORY.toString(),
                new EnumRecommender<>(CacheType.class, e -> e),
                ConfigDef.Importance.MEDIUM, KAFKACACHE_BACKING_CACHE_DOC
            )
            .define(KAFKACACHE_BOUNDED_CACHE_SIZE_CONFIG, ConfigDef.Type.INT, -1,
                ConfigDef.Importance.MEDIUM, KAFKACACHE_BOUNDED_CACHE_SIZE_DOC
            )
            .define(KAFKACACHE_BOUNDED_CACHE_EXPIRY_SECS_CONFIG, ConfigDef.Type.INT, -1,
                ConfigDef.Importance.MEDIUM, KAFKACACHE_BOUNDED_CACHE_EXPIRY_SECS_DOC
            )
            .define(KAFKACACHE_CHECKPOINT_DIR_CONFIG, ConfigDef.Type.STRING, "/tmp",
                ConfigDef.Importance.MEDIUM, KAFKACACHE_CHECKPOINT_DIR_DOC
            )
            .define(KAFKACACHE_CHECKPOINT_VERSION_CONFIG, ConfigDef.Type.INT, 0,
                ConfigDef.Importance.MEDIUM, KAFKACACHE_CHECKPOINT_VERSION_DOC
            )
            .define(KAFKACACHE_DATA_DIR_CONFIG, ConfigDef.Type.STRING, "/tmp",
                ConfigDef.Importance.MEDIUM, KAFKACACHE_DATA_DIR_DOC
            )
            .define(KAFKACACHE_GROUP_ID_CONFIG, ConfigDef.Type.STRING,
                DEFAULT_KAFKACACHE_GROUP_ID_PREFIX + "-" + getDefaultHost(),
                ConfigDef.Importance.LOW, KAFKACACHE_GROUP_ID_DOC
            )
            .define(KAFKACACHE_CLIENT_ID_CONFIG, ConfigDef.Type.STRING, null,
                ConfigDef.Importance.LOW, KAFKACACHE_CLIENT_ID_DOC
            )
            .define(KAFKACACHE_SECURITY_PROTOCOL_CONFIG, ConfigDef.Type.STRING,
                SecurityProtocol.PLAINTEXT.toString(), ConfigDef.Importance.MEDIUM,
                KAFKACACHE_SECURITY_PROTOCOL_DOC
            )
            .define(KAFKACACHE_SSL_TRUSTSTORE_LOCATION_CONFIG, ConfigDef.Type.STRING,
                "", ConfigDef.Importance.HIGH,
                KAFKACACHE_SSL_TRUSTSTORE_LOCATION_DOC
            )
            .define(KAFKACACHE_SSL_TRUSTSTORE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD,
                "", ConfigDef.Importance.HIGH,
                KAFKACACHE_SSL_TRUSTSTORE_PASSWORD_DOC
            )
            .define(KAFKACACHE_SSL_TRUSTSTORE_TYPE_CONFIG, ConfigDef.Type.STRING,
                "JKS", ConfigDef.Importance.MEDIUM,
                KAFAKCACHE_SSL_TRUSTSTORE_TYPE_DOC
            )
            .define(KAFKACACHE_SSL_TRUSTMANAGER_ALGORITHM_CONFIG, ConfigDef.Type.STRING,
                "PKIX", ConfigDef.Importance.LOW,
                KAFKACACHE_SSL_TRUSTMANAGER_ALGORITHM_DOC
            )
            .define(KAFKACACHE_SSL_KEYSTORE_LOCATION_CONFIG, ConfigDef.Type.STRING,
                "", ConfigDef.Importance.HIGH,
                KAFKACACHE_SSL_KEYSTORE_LOCATION_DOC
            )
            .define(KAFKACACHE_SSL_KEYSTORE_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD,
                "", ConfigDef.Importance.HIGH,
                KAFKACACHE_SSL_KEYSTORE_PASSWORD_DOC
            )
            .define(KAFKACACHE_SSL_KEYSTORE_TYPE_CONFIG, ConfigDef.Type.STRING,
                "JKS", ConfigDef.Importance.MEDIUM,
                KAFAKCACHE_SSL_KEYSTORE_TYPE_DOC
            )
            .define(KAFKACACHE_SSL_KEYMANAGER_ALGORITHM_CONFIG, ConfigDef.Type.STRING,
                "SunX509", ConfigDef.Importance.LOW,
                KAFKACACHE_SSL_KEYMANAGER_ALGORITHM_DOC
            )
            .define(KAFKACACHE_SSL_KEY_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD,
                "", ConfigDef.Importance.HIGH,
                KAFKACACHE_SSL_KEY_PASSWORD_DOC
            )
            .define(KAFKACACHE_SSL_ENABLED_PROTOCOLS_CONFIG, ConfigDef.Type.STRING,
                "TLSv1.2,TLSv1.1,TLSv1", ConfigDef.Importance.MEDIUM,
                KAFAKCACHE_SSL_ENABLED_PROTOCOLS_DOC
            )
            .define(KAFKACACHE_SSL_PROTOCOL_CONFIG, ConfigDef.Type.STRING,
                "TLS", ConfigDef.Importance.MEDIUM,
                KAFAKCACHE_SSL_PROTOCOL_DOC
            )
            .define(KAFKACACHE_SSL_PROVIDER_CONFIG, ConfigDef.Type.STRING,
                "", ConfigDef.Importance.MEDIUM,
                KAFAKCACHE_SSL_PROVIDER_DOC
            )
            .define(KAFKACACHE_SSL_CIPHER_SUITES_CONFIG, ConfigDef.Type.STRING,
                "", ConfigDef.Importance.LOW,
                KAFKACACHE_SSL_CIPHER_SUITES_DOC
            )
            .define(KAFKACACHE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ConfigDef.Type.STRING,
                "", ConfigDef.Importance.LOW,
                KAFKACACHE_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC
            )
            .define(KAFKACACHE_SASL_KERBEROS_SERVICE_NAME_CONFIG, ConfigDef.Type.STRING,
                "", ConfigDef.Importance.MEDIUM,
                KAFKACACHE_SASL_KERBEROS_SERVICE_NAME_DOC
            )
            .define(KAFKACACHE_SASL_MECHANISM_CONFIG, ConfigDef.Type.STRING,
                "GSSAPI", ConfigDef.Importance.MEDIUM,
                KAFKACACHE_SASL_MECHANISM_DOC
            )
            .define(KAFKACACHE_SASL_JAAS_CONFIG_CONFIG, ConfigDef.Type.PASSWORD,
                null, ConfigDef.Importance.MEDIUM,
                KAFKACACHE_SASL_JAAS_CONFIG_DOC
            )
            .define(KAFKACACHE_SASL_KERBEROS_KINIT_CMD_CONFIG, ConfigDef.Type.STRING,
                "/usr/bin/kinit", ConfigDef.Importance.LOW,
                KAFKACACHE_SASL_KERBEROS_KINIT_CMD_DOC
            )
            .define(KAFKACACHE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_CONFIG, ConfigDef.Type.LONG,
                60000, ConfigDef.Importance.LOW,
                KAFKACACHE_SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC
            )
            .define(KAFKACACHE_SASL_KERBEROS_TICKET_RENEW_JITTER_CONFIG, ConfigDef.Type.DOUBLE,
                0.05, ConfigDef.Importance.LOW,
                KAFKACACHE_SASL_KERBEROS_TICKET_RENEW_JITTER_DOC
            )
            .define(KAFKACACHE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_CONFIG, ConfigDef.Type.DOUBLE,
                0.8, ConfigDef.Importance.LOW,
                KAFKACACHE_SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC
            );
    }

    public KafkaCacheConfig(String propsFile) {
        this(getPropsFromFile(propsFile));
    }

    public KafkaCacheConfig(Map<?, ?> props) {
        this(config, props);
    }

    public KafkaCacheConfig(ConfigDef configDef, Map<?, ?> props) {
        super(configDef, props);
    }

    public String bootstrapBrokers() {
        List<String> bootstrapServers = getList(KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG);
        String securityProtocol = this.getString(KAFKACACHE_SECURITY_PROTOCOL_CONFIG);

        final Set<String> supportedSecurityProtocols = new HashSet<>(SecurityProtocol.names());
        if (!supportedSecurityProtocols.contains(securityProtocol.toUpperCase(Locale.ROOT))) {
            throw new ConfigException(
                "Only PLAINTEXT, SSL, SASL_PLAINTEXT, and SASL_SSL Kafka endpoints are supported.");
        }

        final String securityProtocolUrlPrefix = securityProtocol + "://";
        final StringBuilder sb = new StringBuilder();
        for (String bootstrapServer : bootstrapServers) {
            if (!bootstrapServer.startsWith(securityProtocolUrlPrefix)) {
                if (bootstrapServer.contains("://")) {
                    log.warn(
                        "Ignoring Kafka broker endpoint " + bootstrapServer + " that does not match the setting for "
                            + KAFKACACHE_SECURITY_PROTOCOL_CONFIG + "=" + securityProtocol);
                    continue;
                } else {
                    bootstrapServer = securityProtocolUrlPrefix + bootstrapServer;
                }
            }

            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(bootstrapServer);
        }

        if (sb.length() == 0) {
            throw new ConfigException("No supported Kafka bootstrap servers are configured.");
        }

        return sb.toString();
    }

    public List<Integer> partitions() {
        List<String> prop = getList(KAFKACACHE_TOPIC_PARTITIONS_CONFIG);
        try {
            return prop.stream()
                .map(Integer::parseInt)
                .collect(Collectors.toList());
        } catch (NumberFormatException e) {
            throw new ConfigException("Couldn't parse partitions: " + prop, e);
        }
    }

    public Offset offset() {
        return new Offset(getString(KAFKACACHE_TOPIC_PARTITIONS_OFFSET_CONFIG));
    }

    public static Properties getPropsFromFile(String propsFile) throws ConfigException {
        Properties props = new Properties();
        if (propsFile == null) {
            return props;
        }
        try (FileInputStream propStream = new FileInputStream(propsFile)) {
            props.load(propStream);
        } catch (IOException e) {
            throw new ConfigException("Couldn't load properties from " + propsFile, e);
        }
        return props;
    }

    private static String getDefaultHost() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            throw new ConfigException("Unknown local hostname", e);
        }
    }

    public enum OffsetType {
        BEGINNING,
        END,
        ABSOLUTE,
        RELATIVE,
        TIMESTAMP;

        private static final Map<String, OffsetType> lookup = new HashMap<>();

        static {
            for (OffsetType v : EnumSet.allOf(OffsetType.class)) {
                lookup.put(v.toString(), v);
            }
        }

        public static OffsetType get(String name) {
            return lookup.get(name.toLowerCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static class Offset {
        private final OffsetType offsetType;
        private final long offset;

        public Offset(String value) {
            try {
                if (value.equalsIgnoreCase(OffsetType.BEGINNING.toString())) {
                    this.offsetType = OffsetType.BEGINNING;
                    this.offset = 0;
                } else if (value.equalsIgnoreCase(OffsetType.END.toString())) {
                    this.offsetType = OffsetType.END;
                    this.offset = 0;
                } else if (value.startsWith("@")) {
                    this.offsetType = OffsetType.TIMESTAMP;
                    this.offset = Long.parseLong(value.substring(1));
                } else {
                    long offset = Long.parseLong(value);
                    if (offset >= 0) {
                        this.offsetType = OffsetType.ABSOLUTE;
                        this.offset = offset;
                    } else {
                        this.offsetType = OffsetType.RELATIVE;
                        this.offset = -offset;
                    }
                }
            } catch (NumberFormatException e) {
                throw new ConfigException("Couldn't parse offset: " + value, e);
            }
        }

        public Offset(OffsetType offsetType, long offset) {
            this.offsetType = offsetType;
            this.offset = offset;
        }

        public OffsetType getOffsetType() {
            return offsetType;
        }

        public long getOffset() {
            return offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Offset offset1 = (Offset) o;
            return offset == offset1.offset && offsetType == offset1.offsetType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(offsetType, offset);
        }

        @Override
        public String toString() {
            switch (offsetType) {
                case BEGINNING:
                    return offsetType.toString();
                case END:
                    return offsetType.toString();
                case ABSOLUTE:
                    return String.valueOf(offset);
                case RELATIVE:
                    return String.valueOf(-offset);
                case TIMESTAMP:
                    return "@" + offset;
                default:
                    throw new IllegalStateException("Invalid offsetType: " + offsetType);
            }
        }
    }
}
