/*-
 * #%L
 * LmdbJava Benchmarks
 * %%
 * Copyright (C) 2016 - 2020 The LmdbJava Open Source Project
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package io.kcache.benchmark;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Level.Invocation;
import static org.openjdk.jmh.annotations.Level.Trial;
import static org.openjdk.jmh.annotations.Mode.SampleTime;
import static org.openjdk.jmh.annotations.Scope.Benchmark;

import io.kcache.CacheType;
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import io.kcache.bdbje.BdbJECache;
import io.kcache.lmdb.LmdbCache;
import io.kcache.mapdb.MapDBCache;
import io.kcache.rocksdb.RocksDBCache;
import io.kcache.utils.PersistentCache;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

@OutputTimeUnit(MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(SampleTime)
@SuppressWarnings({"checkstyle:javadoctype", "checkstyle:designforextension"})
public class PersistentCacheBenchmark {

    @Benchmark
    public void readKey(final Reader r, final Blackhole bh) {
        for (final int key : r.keys) {
            if (r.intKey) {
                r.wkb.putInt(0, key);
            } else {
                r.wkb.putStringWithoutLengthUtf8(0, r.padKey(key));
            }
            bh.consume(r.cache.get(r.wkb.byteArray()));
        }
    }

    @Benchmark
    public void readRev(final Reader r, final Blackhole bh) {
        try (KeyValueIterator<byte[], byte[]> iterator = r.cache.descendingCache().all()) {
            while (iterator.hasNext()) {
                final KeyValue<byte[], byte[]> entry = iterator.next();
                bh.consume(entry.value);
            }
        }
    }

    @Benchmark
    public void readSeq(final Reader r, final Blackhole bh) {
        try (KeyValueIterator<byte[], byte[]> iterator = r.cache.all()) {
            while (iterator.hasNext()) {
                final KeyValue<byte[], byte[]> entry = iterator.next();
                bh.consume(entry.value);
            }
        }
    }

    @Benchmark
    public void write(final Writer w, final Blackhole bh) {
        w.write();
    }

    @State(value = Benchmark)
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public static class CommonKafkaCache extends Common {

        @Param({"bdbje", "lmdb", "mapdb", "rocksdb"})
        String cacheType;

        PersistentCache<byte[], byte[]> cache;

        /**
         * Writable key buffer. Backed by a plain byte[] for MapDb API ease.
         */
        MutableDirectBuffer wkb;

        /**
         * Writable value buffer. Backed by a plain byte[] for MapDb API ease.
         */
        MutableDirectBuffer wvb;

        @Override
        public void setup(final BenchmarkParams b) throws IOException {
            super.setup(b);
            wkb = new UnsafeBuffer(new byte[keySize]);
            wvb = new UnsafeBuffer(new byte[valSize]);

            cache = createCache(cacheType);
            cache.init();
        }

        @Override
        public void teardown() throws IOException {
            reportSpaceBeforeClose();
            cache.close();
            super.teardown();
        }

        void write() {
            final int rndByteMax = RND_MB.length - valSize;
            int rndByteOffset = 0;
            for (final int key : keys) {
                if (intKey) {
                    wkb.putInt(0, key, LITTLE_ENDIAN);
                } else {
                    wkb.putStringWithoutLengthUtf8(0, padKey(key));
                }
                if (valRandom) {
                    wvb.putBytes(0, RND_MB, rndByteOffset, valSize);
                    rndByteOffset += valSize;
                    if (rndByteOffset >= rndByteMax) {
                        rndByteOffset = 0;
                    }
                } else {
                    wvb.putInt(0, key);
                }
                cache.put(wkb.byteArray(), wvb.byteArray());
            }
            cache.flush();
        }

        private PersistentCache<byte[], byte[]> createCache(String cacheType) {
            String name = "default";
            String dataDir = new File("/tmp", cacheType + UUID.randomUUID()).getAbsolutePath();
            Serde<byte[]> serde = Serdes.ByteArray();
            switch (CacheType.get(cacheType)) {
                case BDBJE:
                    return new BdbJECache<>(name, dataDir, serde, serde, null);
                case LMDB:
                    return new LmdbCache<>(name, dataDir, serde, serde, null);
                case MAPDB:
                    return new MapDBCache<>(name, dataDir, serde, serde, null);
                case ROCKSDB:
                    return new RocksDBCache<>(name, dataDir, serde, serde, null);
                default:
                    return null;
            }
        }
    }

    @State(Benchmark)
    public static class Reader extends CommonKafkaCache {

        @Setup(Trial)
        @Override
        public void setup(final BenchmarkParams b) throws IOException {
            super.setup(b);
            super.write();
        }

        @TearDown(Trial)
        @Override
        public void teardown() throws IOException {
            super.teardown();
        }
    }

    @State(Benchmark)
    public static class Writer extends CommonKafkaCache {

        @Setup(Invocation)
        @Override
        public void setup(final BenchmarkParams b) throws IOException {
            super.setup(b);
        }

        @TearDown(Invocation)
        @Override
        public void teardown() throws IOException {
            super.teardown();
        }
    }
}
