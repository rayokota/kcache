# KCache Persistent Cache JMH Microbenchmarks

This module is for JMH micro-benchmarking persistent caches in KCache.

### How to run

The benchmarks can be run either from `PersistentCacheBenchmark.java` directly through IntelliJ, or via the
command line as follows, after building the module to produce `target/benchmarks.jar`:

```
java -jar ./target/benchmarks.jar
```

### Running a subset of benchmarks

To run only a subset of the benchmarks, you can specify parameters to run with. For example,
to run only `rocksdb` benchmarks:
```
java -jar ./target/benchmarks.jar -p cacheType=rocksdb
```

### Running with non-default parameters

JMH parameters of interest may include the number of forks to use (`-f`), the number of warmup and
measurement iterations (`-wi` and `-i`, respectively), the duration of each iteration
(`-w` and `-r` for warmup and measurement iterations, respectively, with units of seconds),
and the number of threads (`-t`).
By default, `PersistentCacheBenchmark.java` is set up to run with 1 fork, 3 warmup iterations, and 3 measurement
iterations.

As an example, to run benchmarks with only one warmup iteration:
```
java -jar ./target/benchmarks.jar -wi 1
```

The full list of JMH command line options can be viewed with:
```
java -jar ./target/benchmarks.jar -h
```

