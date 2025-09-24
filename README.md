# KCache - An In-Memory Cache Backed by Apache Kafka

[![Build Status][github-actions-shield]][github-actions-link]
[![Maven][maven-shield]][maven-link]
[![Javadoc][javadoc-shield]][javadoc-link]

[github-actions-shield]: https://github.com/rayokota/kcache/workflows/build/badge.svg?branch=master
[github-actions-link]: https://github.com/rayokota/kcache/actions
[maven-shield]: https://img.shields.io/maven-central/v/io.kcache/kcache.svg
[maven-link]: https://search.maven.org/#search%7Cga%7C1%7Cio.kcache
[javadoc-shield]: https://javadoc.io/badge/io.kcache/kcache.svg?color=blue
[javadoc-link]: https://javadoc.io/doc/io.kcache/kcache

KCache is a client library that provides an in-memory cache backed by a compacted topic in Kafka.  It is one of the patterns for using Kafka  as a persistent store, as described by Jay Kreps in the article [It's Okay to Store Data in Apache Kafka](https://www.confluent.io/blog/okay-store-data-apache-kafka/).

## Maven

Releases of KCache are deployed to Maven Central.

```xml
<dependency>
    <groupId>io.kcache</groupId>
    <artifactId>kcache</artifactId>
    <version>5.2.3</version>
</dependency>
```

For Java 11 or above, use `5.x` otherwise use `4.x`.

## Usage

An instance of `KafkaCache` implements the `java.util.SortedMap` interface.  Here is an example usage:

```java
import io.kcache.*;

String bootstrapServers = "localhost:9092";
Cache<String, String> cache = new KafkaCache<>(
    bootstrapServers,
    Serdes.String(),  // for serializing/deserializing keys
    Serdes.String()   // for serializing/deserializing values
);
cache.init();   // creates topic, initializes cache, consumer, and producer
cache.put("Kafka", "Rocks");
String value = cache.get("Kafka");  // returns "Rocks"
cache.remove("Kafka");
cache.close();  // shuts down the cache, consumer, and producer
```

One can also use RocksDB to back the `KafkaCache`:

```java
Properties props = new Properties();
props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
props.put(KafkaCacheConfig.KAFKACACHE_BACKING_CACHE_CONFIG, "rocksdb");
props.put(KafkaCacheConfig.KAFKACACHE_DATA_DIR_CONFIG, "/tmp");
Cache<String, String> cache = new KafkaCache<>(
    new KafkaCacheConfig(props),
    Serdes.String(),  // for serializing/deserializing keys
    Serdes.String()   // for serializing/deserializing values
);
cache.init();
```
## Basic Configuration

KCache has a number of configuration properties that can be specified.

- `kafkacache.bootstrap.servers` - A list of host and port pairs to use for establishing the initial connection to Kafka.
- `kafkacache.group.id` - The group ID to use for the internal consumer.  Defaults to `kafkacache`.
- `kafkacache.client.id` - The client ID to use for the internal consumer.  Defaults to `kafka-cache-reader-<topic>`.
- `kafkacache.topic` - The name of the compacted topic.  Defaults to `_cache`.
- `kafkacache.topic.replication.factor` - The desired replication factor for the compacted topic.  Defaults to 3.
- `kafkacache.topic.num.partitions` - The desired number of partitions for for the compacted topic.  Defaults to 1.
- `kafkacache.topic.partitions` - A list of partitions to consume, or all partitions if not specified.
- `kafkacache.topic.partitions.offset` - The offset to start consuming all partitions from, one of `beginning`, `end`, 
   a positive number representing an absolute offset, a negative number representing a relative offset from the end, 
   or `@<value>`, where `<value>` is a timestamp in ms.  Defaults to `beginning`.
- `kafkacache.init.timeout.ms` - The timeout for initialization of the Kafka cache, including creation of the compacted topic.  Defaults to 300 seconds.
- `kafkacache.timeout.ms` - The timeout for an operation on the Kafka cache.  Defaults to 60 seconds.
- `kafkacache.backing.cache` - The backing cache for KCache, one of `memory` (default), `bdbje`, `caffeine`, `lmdb`, `mapdb`, `rdbms`, or `rocksdb`.
- `kafkacache.data.dir` - The root directory for backing cache storage.  Defaults to `/tmp`.

Configuration properties can be passed as follows:

```java
Properties props = new Properties();
props.setProperty("kafkacache.bootstrap.servers", "localhost:9092");
props.setProperty("kafkacache.topic", "_mycache");
Cache<String, String> cache = new KafkaCache<>(
    new KafkaCacheConfig(props),
    Serdes.String(),  // for serializing/deserializing keys
    Serdes.String()   // for serializing/deserializing values
);
cache.init();
...
```

## Security

KCache supports both SSL authentication and SASL authentication to a secure Kafka cluster.  See the [JavaDoc](https://static.javadoc.io/io.kcache/kcache/latest/io/kcache/KafkaCacheConfig.html) for more information.

## Using KCache as a Replicated Cache

KCache can be used as a replicated cache, with some caveats.  To ensure that updates are processed in the proper order, one instance of KCache should be designated as the sole writer, with all writes being forwarded to it.  If the writer fails, another instance can then be elected as the new writer.  

For an example of a highly-available service that wraps KCache, see [Keta](https://github.com/rayokota/keta).
