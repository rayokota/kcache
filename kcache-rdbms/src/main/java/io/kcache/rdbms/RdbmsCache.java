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

import static io.kcache.rdbms.jooq.Tables.KV;
import static org.jooq.impl.DSL.constraint;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import io.kcache.KeyValueIterators;
import io.kcache.exceptions.CacheInitializationException;
import io.kcache.rdbms.jooq.Kcache;
import io.kcache.rdbms.jooq.tables.records.KvRecord;
import io.kcache.utils.PersistentCache;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.apache.kafka.common.serialization.Serde;
import org.jooq.Condition;
import org.jooq.Cursor;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.SelectOrderByStep;
import org.jooq.SelectWhereStep;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.NoDataFoundException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.util.derby.DerbyDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A persistent key-value store based on a relational database.
 */
public class RdbmsCache<K, V> extends PersistentCache<K, V> {
    private static final Logger log = LoggerFactory.getLogger(RdbmsCache.class);

    public static final String DIALECT_CONFIG = "dialect";
    public static final String JDBC_URL_CONFIG = "jdbcUrl";
    public static final String PASSWORD_CONFIG = "password";
    public static final String USERNAME_CONFIG = "username";

    private static final String DB_FILE_DIR = "rdbms";

    private String jdbcUrl;
    private SQLDialect dialect;
    private String username;
    private String password;
    private DataSource ds;

    private final Set<KeyValueIterator<K, V>> openIterators = ConcurrentHashMap.newKeySet();

    public RdbmsCache(final String name,
                      final String rootDir,
                      Serde<K> keySerde,
                      Serde<V> valueSerde) {
        this(name, DB_FILE_DIR, rootDir, keySerde, valueSerde);
    }

    public RdbmsCache(final String name,
                      final String rootDir,
                      Serde<K> keySerde,
                      Serde<V> valueSerde,
                      Comparator<K> comparator) {
        this(name, DB_FILE_DIR, rootDir, keySerde, valueSerde, comparator);
    }

    public RdbmsCache(final String name,
                      final String parentDir,
                      final String rootDir,
                      Serde<K> keySerde,
                      Serde<V> valueSerde) {
        this(name, parentDir, rootDir, keySerde, valueSerde, null);
    }

    public RdbmsCache(final String name,
                      final String parentDir,
                      final String rootDir,
                      Serde<K> keySerde,
                      Serde<V> valueSerde,
                      Comparator<K> comparator) {
        super(name, parentDir, rootDir, keySerde, valueSerde, comparator);
    }

    private DSLContext dsl() {
        return DSL.using(ds, dialect);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.jdbcUrl = (String) configs.get(JDBC_URL_CONFIG);
        this.username = (String) configs.get(USERNAME_CONFIG);
        this.password = (String) configs.get(PASSWORD_CONFIG);
        String dialect = (String) configs.get(DIALECT_CONFIG);
        this.dialect = SQLDialect.valueOf(dialect);
    }

    @Override
    protected void openDB() {
        try {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(jdbcUrl);
            config.setUsername(username);
            if (password != null) {
                config.setPassword(password);
            }
            ds = new HikariDataSource(config);

            tryCreateSchema();
            tryCreateTable();
        } catch (final Exception e) {
            throw new CacheInitializationException("Error opening store " + name() + " at location " + dbDir(), e);
        }
    }

    private void tryCreateSchema() {
        try {
            dsl().createSchemaIfNotExists(Kcache.KCACHE)
                .execute();
        } catch (DataAccessException dae) {
            log.warn("Could not create schema");
        }
    }

    private void tryCreateTable() {
        try {
            switch (dialect) {
                case DERBY:
                    dsl().createTableIfNotExists(KV)
                        .column(KV.KV_KEY.getName(),
                            DerbyDataType.CHARVARYINGFORBITDATA.nullable(false))
                        .column(KV.KV_VALUE.getName(),
                            DerbyDataType.CHARVARYINGFORBITDATA.nullable(false))
                        .constraints(constraint("PK_KV").primaryKey(KV.KV_KEY))
                        .execute();
                    break;
                default:
                    dsl().createTableIfNotExists(KV)
                        .column(KV.KV_KEY.getName(),
                            // Max key size for MySQL is 3072
                            SQLDataType.VARBINARY.length(3072).nullable(false))
                        .column(KV.KV_VALUE.getName(),
                            SQLDataType.VARBINARY.length(32672).nullable(false))
                        .constraints(constraint("PK_KV").primaryKey(KV.KV_KEY))
                        .execute();
                    break;
            }
        } catch (DataAccessException dae) {
            log.warn("Could not create table");
        }
    }

    @Override
    public int size() {
        validateStoreOpen();
        return dsl().selectCount()
            .from(KV)
            .fetchOne(0, int.class);
    }

    @Override
    public V put(final K key, final V value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        final V originalValue = get(key);
        byte[] keyBytes = keySerde().serializer().serialize(null, key);
        byte[] valueBytes = valueSerde().serializer().serialize(null, value);
        dsl().insertInto(KV, KV.KV_KEY, KV.KV_VALUE)
            .values(keyBytes, valueBytes)
            .onDuplicateKeyUpdate()
            .set(KV.KV_VALUE, valueBytes)
            .execute();
        return originalValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        validateStoreOpen();
        dsl().transaction(config -> {
            for (Map.Entry<? extends K, ? extends V> entry : entries.entrySet()) {
                byte[] keyBytes = keySerde().serializer().serialize(null, entry.getKey());
                byte[] valueBytes = valueSerde().serializer().serialize(null, entry.getValue());
                DSL.using(config).insertInto(KV, KV.KV_KEY, KV.KV_VALUE)
                    .values(keyBytes, valueBytes)
                    .onDuplicateKeyUpdate()
                    .set(KV.KV_VALUE, valueBytes)
                    .execute();
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(final Object key) {
        validateStoreOpen();
        byte[] keyBytes = keySerde().serializer().serialize(null, (K) key);
        try {
            KvRecord row = dsl().selectFrom(KV)
                .where(KV.KV_KEY.eq(keyBytes))
                .fetchSingle();
            byte[] valueBytes = row.getKvValue();
            return valueSerde().deserializer().deserialize(null, valueBytes);
        } catch (NoDataFoundException e) {
            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(final Object key) {
        Objects.requireNonNull(key, "key cannot be null");
        final V originalValue = get(key);
        byte[] keyBytes = keySerde().serializer().serialize(null, (K) key);
        dsl().deleteFrom(KV)
            .where(KV.KV_KEY.eq(keyBytes))
            .execute();
        return originalValue;
    }

    @Override
    protected KeyValueIterator<K, V> range(K from, boolean fromInclusive, K to, boolean toInclusive, boolean isDescending) {
        validateStoreOpen();

        Condition fromCond = null;
        if (from != null) {
            byte[] fromBytes = keySerde().serializer().serialize(null, from);
            fromCond = fromInclusive
                ? isDescending ? KV.KV_KEY.le(fromBytes) : KV.KV_KEY.ge(fromBytes)
                : isDescending ? KV.KV_KEY.lt(fromBytes) : KV.KV_KEY.gt(fromBytes);
        }
        Condition toCond = null;
        if (to != null) {
            byte[] toBytes = keySerde().serializer().serialize(null, to);
            toCond = toInclusive
                ? isDescending ? KV.KV_KEY.ge(toBytes) : KV.KV_KEY.le(toBytes)
                : isDescending ? KV.KV_KEY.gt(toBytes) : KV.KV_KEY.lt(toBytes);
        }
        Condition whereCond;
        if (fromCond != null) {
            if (toCond != null) {
                whereCond = fromCond.and(toCond);
            } else {
                whereCond = fromCond;
            }
        } else {
            whereCond = toCond;
        }
        SelectWhereStep<KvRecord> selectWhere = dsl().selectFrom(KV);
        SelectOrderByStep<KvRecord> selectOrderBy = whereCond != null
            ? selectWhere.where(whereCond)
            : selectWhere;
        Cursor<KvRecord> cursor = selectOrderBy
            .orderBy(isDescending? KV.KV_KEY.desc() : KV.KV_KEY.asc())
            .fetchLazy();

        KeyValueIterator<byte[], byte[]> iter = new KeyValueIterator<byte[], byte[]>() {

            @Override
            public boolean hasNext() {
                return cursor.hasNext();
            }

            @Override
            public KeyValue<byte[], byte[]> next() {
                KvRecord record = cursor.fetchNext();
                return new KeyValue<>(record.getKvKey(), record.getKvValue());
            }

            @Override
            public void close() {
                cursor.close();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        return KeyValueIterators.transformRawIterator(keySerde(), valueSerde(), iter);
    }

    @Override
    protected KeyValueIterator<K, V> all(boolean isDescending) {
        return range(null, true, null, true, isDescending);
    }

    @Override
    public void flush() {
    }

    @Override
    public void clear() {
        // For testing
        dsl().deleteFrom(KV).execute();
    }

    @Override
    protected void closeDB() {
    }
}
