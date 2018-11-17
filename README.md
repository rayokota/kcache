# KCache - An In-Memory Cache Backed by Kafka

KCache is a client library that provides an in-memory cache backed by a compacted topic in Kafka.  It is one of the ways to use Kafka as a persistent store as described by Jay Kreps in [It's Okay to Store Data in Apache Kafka](https://www.confluent.io/blog/okay-store-data-apache-kafka/).  

## Installing

Releases of KCache are deployed to Maven Central.

```xml
<dependency>
    <groupId>io.kcache</groupId>
    <artifactId>kcache</artifactId>
    <version>0.0.1</version>
</dependency>
```

## Usage

An instance of `KafkaCache` implements the `java.util.Map` interface.  Here is an example usage:

```java
String bootstrapServers = "localhost:9092";
KafkaCache<String, String> cache = new KafkaCache<>(
    bootstrapServers,
    Serdes.String(),  // for serializing/deserializing keys
    Serdes.String()   // for serializing/deserializing values
);
cache.init();   // initializes the internal consumer and producer
cache.put("Kafka", "Rocks");
String value = cache.get("Kafka");  // returns "Rocks"
cache.remove("Kafka");
cache.close();  // shuts down the internal consumer and producer
```

## Configuration

KCache has a number of configuration properties that can be specified.

- `kafkacache.bootstrap.servers` - A list of host and port pairs to use for establishing the initial connection to Kafka.
- `kafkacache.group.id` - The group ID to use for the internal consumer.  Defaults to `kafkacache`.
- `kafkacache.topic` - The name of the compacted topic.  Defaults to `_cache`.
- `kafkacache.topic.replication.factor` - The replication factor for the compacted topic.  Defaults to 3.
- `kafkacache.timeout.ms` - The timeout for initialization of the Kafka cache, including creation of the compacted topic.  Defaults to 60 seconds.
- `kafkacache.internal.timeout.ms` - The timeout for an operation on the Kafka cache.  Defaults to 500 milliseconds.

Configuration properties can be passed as follows:

```
Properties props = new Properties();
props.setProperty("kafkacache.bootstrap.servers", "localhost:9092");
props.setProperty("kafkacache.topic", "_mycache");
KafkaCache<String, String> cache = new KafkaCache<>(
    new KafkaCacheConfig(props),
    Serdes.String(),  // for serializing/deserializing keys
    Serdes.String()   // for serializing/deserializing values
);
cache.init();   // initializes the internal consumer and producer
...
```
