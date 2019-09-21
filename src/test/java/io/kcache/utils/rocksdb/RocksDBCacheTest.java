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
package io.kcache.utils.rocksdb;

import io.kcache.Cache;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RocksDBCacheTest {
    private static boolean enableBloomFilters = false;
    private final static String DB_NAME = "db-name";

    private File dir;
    private final Serializer<String> stringSerializer = new StringSerializer();
    private final Deserializer<String> stringDeserializer = new StringDeserializer();

    RocksDBCache<Bytes, byte[]> RocksDBCache;

    @Before
    public void setUp() {
        dir = TestUtils.tempDirectory();
        RocksDBCache = getRocksDBCache();
    }

    RocksDBCache<Bytes, byte[]> getRocksDBCache() {
        return new RocksDBCache<>(DB_NAME, dir.getAbsolutePath(), Serdes.Bytes(), Serdes.ByteArray());
    }

    @After
    public void tearDown() {
        RocksDBCache.close();
    }

    @Test
    public void shouldPutAll() {
        final Map<Bytes, byte[]> entries = new HashMap<>();
        entries.put(
            new Bytes(stringSerializer.serialize(null, "1")),
            stringSerializer.serialize(null, "a"));
        entries.put(
            new Bytes(stringSerializer.serialize(null, "2")),
            stringSerializer.serialize(null, "b"));
        entries.put(
            new Bytes(stringSerializer.serialize(null, "3")),
            stringSerializer.serialize(null, "c"));
        entries.put(
            new Bytes(stringSerializer.serialize(null, "4")),
            stringSerializer.serialize(null, "d"));

        RocksDBCache.init();
        RocksDBCache.putAll(entries);
        RocksDBCache.flush();

        assertEquals(
            "a",
            stringDeserializer.deserialize(
                null,
                RocksDBCache.get(new Bytes(stringSerializer.serialize(null, "1")))));
        assertEquals(
            "b",
            stringDeserializer.deserialize(
                null,
                RocksDBCache.get(new Bytes(stringSerializer.serialize(null, "2")))));
        assertEquals(
            "c",
            stringDeserializer.deserialize(
                null,
                RocksDBCache.get(new Bytes(stringSerializer.serialize(null, "3")))));
        assertEquals(
            "d",
            stringDeserializer.deserialize(
                null,
                RocksDBCache.get(new Bytes(stringSerializer.serialize(null, "4")))));
    }

    @Test
    public void shouldPutOnlyIfAbsentValue() {
        RocksDBCache.init();
        final Bytes keyBytes = new Bytes(stringSerializer.serialize(null, "one"));
        final byte[] valueBytes = stringSerializer.serialize(null, "A");
        final byte[] valueBytesUpdate = stringSerializer.serialize(null, "B");

        RocksDBCache.putIfAbsent(keyBytes, valueBytes);
        RocksDBCache.putIfAbsent(keyBytes, valueBytesUpdate);

        final String retrievedValue = stringDeserializer.deserialize(null, RocksDBCache.get(keyBytes));
        assertEquals("A", retrievedValue);
    }

    @Test
    public void shouldReturnSubCaches() {
        final Map<Bytes, byte[]> entries = new HashMap<>();
        entries.put(
            new Bytes(stringSerializer.serialize(null, "1")),
            stringSerializer.serialize(null, "a"));
        entries.put(
            new Bytes(stringSerializer.serialize(null, "2")),
            stringSerializer.serialize(null, "b"));
        entries.put(
            new Bytes(stringSerializer.serialize(null, "3")),
            stringSerializer.serialize(null, "c"));
        entries.put(
            new Bytes(stringSerializer.serialize(null, "4")),
            stringSerializer.serialize(null, "d"));

        RocksDBCache.init();
        RocksDBCache.putAll(entries);
        RocksDBCache.flush();

        Cache<Bytes, byte[]> subCache = RocksDBCache.subCache(
            new Bytes(stringSerializer.serialize(null, "2")),
            true,
            new Bytes(stringSerializer.serialize(null, "3")),
            true);

        assertEquals(2, subCache.size());
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "1"))));
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "4"))));

        subCache = RocksDBCache.subCache(
            new Bytes(stringSerializer.serialize(null, "2")),
            true,
            new Bytes(stringSerializer.serialize(null, "4")),
            false);

        assertEquals(2, subCache.size());
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "1"))));
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "4"))));

        subCache = RocksDBCache.subCache(
            new Bytes(stringSerializer.serialize(null, "1")),
            false,
            new Bytes(stringSerializer.serialize(null, "3")),
            true);

        assertEquals(2, subCache.size());
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "1"))));
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "4"))));

        subCache = RocksDBCache.subCache(
            new Bytes(stringSerializer.serialize(null, "1")),
            false,
            new Bytes(stringSerializer.serialize(null, "4")),
            false);

        assertEquals(2, subCache.size());
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "1"))));
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "4"))));

        subCache = RocksDBCache.subCache(
            null,
            false,
            new Bytes(stringSerializer.serialize(null, "4")),
            false);

        assertEquals(3, subCache.size());
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "4"))));

        subCache = RocksDBCache.subCache(
            new Bytes(stringSerializer.serialize(null, "1")),
            false,
            null,
            false);

        assertEquals(3, subCache.size());
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "1"))));
    }
}
