# KCache - An In-Memory Cache Backed by Apache Kafka

[![Build Status][travis-shield]][travis-link]
[![Maven][maven-shield]][maven-link]
[![Javadoc][javadoc-shield]][javadoc-link]

[travis-shield]: https://travis-ci.org/rayokota/kcache.svg?branch=master
[travis-link]: https://travis-ci.org/rayokota/kcache
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
    <version>3.3.1</version>
</dependency>
```

## Usage

An instance of `KafkaCache` implements the `java.util.Map` interface.  Here is an example usage:

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
- `kafkacache.topic.replication.factor` - The replication factor for the compacted topic.  Defaults to 3.
- `kafkacache.init.timeout.ms` - The timeout for initialization of the Kafka cache, including creation of the compacted topic.  Defaults to 300 seconds.
- `kafkacache.timeout.ms` - The timeout for an operation on the Kafka cache.  Defaults to 60 seconds.
- `kafkacache.backing.cache` - The backing cache for KCache, one of `memory` (default), `bdbje`, `lmdb`, or `rocksdb`.
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

KCache supports both SSL authentication and SASL authentication to a secure Kafka cluster.  See the [JavaDoc](https://static.javadoc.io/io.kcache/kcache/2.0.2/io/kcache/KafkaCacheConfig.html) for more information.

## Using KCache as a Replicated Cache

KCache can be used as a replicated cache, with some caveats.  To ensure that updates are processed in the proper order, one instance of KCache should be designated as the sole writer, with all writes being forwarded to it.  If the writer fails, another instance can then be elected as the new writer.  The leader election is outside of KCache but can be implemented using [ZooKeeper](https://zookeeper.apache.org/doc/current/recipes.html#sc_leaderElection), for example.  Reads of course can be served by any instance of KCache.
