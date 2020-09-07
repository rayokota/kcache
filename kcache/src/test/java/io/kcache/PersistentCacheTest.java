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
package io.kcache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public abstract class PersistentCacheTest {
    protected final static String DB_NAME = "db-name";

    @Rule
    public final TemporaryFolder dir = new TemporaryFolder();

    private final Serializer<String> stringSerializer = new StringSerializer();
    private final Deserializer<String> stringDeserializer = new StringDeserializer();

    Cache<Bytes, byte[]> cache;

    protected abstract Cache<Bytes, byte[]> getCache();

    @Before
    public void setUp() {
        cache = getCache();
    }

    @After
    public void tearDown() throws Exception {
        cache.close();
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

        cache.init();
        cache.putAll(entries);
        cache.flush();

        assertEquals(
            "a",
            stringDeserializer.deserialize(
                null,
                cache.get(new Bytes(stringSerializer.serialize(null, "1")))));
        assertEquals(
            "b",
            stringDeserializer.deserialize(
                null,
                cache.get(new Bytes(stringSerializer.serialize(null, "2")))));
        assertEquals(
            "c",
            stringDeserializer.deserialize(
                null,
                cache.get(new Bytes(stringSerializer.serialize(null, "3")))));
        assertEquals(
            "d",
            stringDeserializer.deserialize(
                null,
                cache.get(new Bytes(stringSerializer.serialize(null, "4")))));
    }

    @Test
    public void shouldPutOnlyIfAbsentValue() {
        cache.init();
        final Bytes keyBytes = new Bytes(stringSerializer.serialize(null, "one"));
        final byte[] valueBytes = stringSerializer.serialize(null, "A");
        final byte[] valueBytesUpdate = stringSerializer.serialize(null, "B");

        cache.putIfAbsent(keyBytes, valueBytes);
        cache.putIfAbsent(keyBytes, valueBytesUpdate);

        final String retrievedValue = stringDeserializer.deserialize(null, cache.get(keyBytes));
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

        cache.init();
        cache.putAll(entries);
        cache.flush();

        Cache<Bytes, byte[]> subCache = cache.subCache(
            new Bytes(stringSerializer.serialize(null, "2")),
            true,
            new Bytes(stringSerializer.serialize(null, "3")),
            true);

        assertEquals(2, subCache.size());
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "1"))));
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "4"))));

        subCache = cache.subCache(
            new Bytes(stringSerializer.serialize(null, "2")),
            true,
            new Bytes(stringSerializer.serialize(null, "4")),
            false);

        assertEquals(2, subCache.size());
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "1"))));
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "4"))));

        subCache = cache.subCache(
            new Bytes(stringSerializer.serialize(null, "1")),
            false,
            new Bytes(stringSerializer.serialize(null, "3")),
            true);

        assertEquals(2, subCache.size());
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "1"))));
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "4"))));

        subCache = cache.subCache(
            new Bytes(stringSerializer.serialize(null, "1")),
            false,
            new Bytes(stringSerializer.serialize(null, "4")),
            false);

        assertEquals(2, subCache.size());
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "1"))));
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "4"))));

        subCache = cache.subCache(
            null,
            false,
            new Bytes(stringSerializer.serialize(null, "4")),
            false);

        assertEquals(3, subCache.size());
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "4"))));

        subCache = cache.subCache(
            new Bytes(stringSerializer.serialize(null, "1")),
            false,
            null,
            false);

        assertEquals(3, subCache.size());
        assertNull(subCache.get(new Bytes(stringSerializer.serialize(null, "1"))));

        Cache<Bytes, byte[]> descendingCache = cache.descendingCache();
        KeyValueIterator<Bytes, byte[]> iter = descendingCache.all();
        KeyValue<Bytes, byte[]> kv = iter.next();
        assertEquals("4", stringDeserializer.deserialize(null, kv.key.get()));
        assertEquals("d", stringDeserializer.deserialize(null, kv.value));

        subCache = descendingCache.subCache(
            new Bytes(stringSerializer.serialize(null, "3")),
            true,
            new Bytes(stringSerializer.serialize(null, "2")),
            true);
        iter = subCache.all();
        kv = iter.next();
        assertEquals("3", stringDeserializer.deserialize(null, kv.key.get()));
        assertEquals("c", stringDeserializer.deserialize(null, kv.value));
        kv = iter.next();
        assertEquals("2", stringDeserializer.deserialize(null, kv.key.get()));
        assertEquals("b", stringDeserializer.deserialize(null, kv.value));

        subCache = descendingCache.subCache(
            new Bytes(stringSerializer.serialize(null, "4")),
            false,
            new Bytes(stringSerializer.serialize(null, "2")),
            true);
        iter = subCache.all();
        kv = iter.next();
        assertEquals("3", stringDeserializer.deserialize(null, kv.key.get()));
        assertEquals("c", stringDeserializer.deserialize(null, kv.value));
        kv = iter.next();
        assertEquals("2", stringDeserializer.deserialize(null, kv.key.get()));
        assertEquals("b", stringDeserializer.deserialize(null, kv.value));

        subCache = descendingCache.subCache(
            new Bytes(stringSerializer.serialize(null, "3")),
            true,
            new Bytes(stringSerializer.serialize(null, "1")),
            false);
        iter = subCache.all();
        kv = iter.next();
        assertEquals("3", stringDeserializer.deserialize(null, kv.key.get()));
        assertEquals("c", stringDeserializer.deserialize(null, kv.value));
        kv = iter.next();
        assertEquals("2", stringDeserializer.deserialize(null, kv.key.get()));
        assertEquals("b", stringDeserializer.deserialize(null, kv.value));

        subCache = descendingCache.subCache(
            new Bytes(stringSerializer.serialize(null, "4")),
            false,
            new Bytes(stringSerializer.serialize(null, "1")),
            false);
        iter = subCache.all();
        kv = iter.next();
        assertEquals("3", stringDeserializer.deserialize(null, kv.key.get()));
        assertEquals("c", stringDeserializer.deserialize(null, kv.value));
        kv = iter.next();
        assertEquals("2", stringDeserializer.deserialize(null, kv.key.get()));
        assertEquals("b", stringDeserializer.deserialize(null, kv.value));

        subCache = subCache.descendingCache();
        iter = subCache.all();
        kv = iter.next();
        assertEquals("2", stringDeserializer.deserialize(null, kv.key.get()));
        assertEquals("b", stringDeserializer.deserialize(null, kv.value));
        kv = iter.next();
        assertEquals("3", stringDeserializer.deserialize(null, kv.key.get()));
        assertEquals("c", stringDeserializer.deserialize(null, kv.value));


        subCache = descendingCache.subCache(
            new Bytes(stringSerializer.serialize(null, "31")),  // tests seekForPrev
            true,
            new Bytes(stringSerializer.serialize(null, "1")),
            true);
        iter = subCache.all();
        kv = iter.next();
        assertEquals("3", stringDeserializer.deserialize(null, kv.key.get()));
        assertEquals("c", stringDeserializer.deserialize(null, kv.value));
        kv = iter.next();
        assertEquals("2", stringDeserializer.deserialize(null, kv.key.get()));
        assertEquals("b", stringDeserializer.deserialize(null, kv.value));

    }
}
