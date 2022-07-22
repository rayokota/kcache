package io.kcache.utils;

import com.google.common.util.concurrent.Striped;
import io.kcache.Cache;
import io.kcache.KeyValueIterator;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class Caches {

    public static <K, V> Cache<K, V> concurrentCache(Cache<K, V> m) {
        return new ConcurrentCache<>(m);
    }

    private static class ConcurrentCache<K, V>
        implements Cache<K, V>, Serializable {
        private static final long serialVersionUID = 1978198479659022715L;

        private final Cache<K, V> m;     // Backing cache
        final transient Striped<ReadWriteLock> striped;        // Object on which to synchronize

        ConcurrentCache(Cache<K, V> m) {
            this.m = Objects.requireNonNull(m);
            striped = Striped.readWriteLock(128);
        }

        ConcurrentCache(Cache<K, V> m, Striped<ReadWriteLock> striped) {
            this.m = m;
            this.striped = striped;
        }

        public Comparator<? super K> comparator() {
            return m.comparator();
        }

        public boolean isPersistent() {
            return m.isPersistent();
        }

        public synchronized void init() {
            m.init();
        }

        public synchronized void sync() {
            m.sync();
        }

        public int size() {
            return m.size();
        }

        public boolean isEmpty() {
            return m.isEmpty();
        }

        public boolean containsKey(Object key) {
            Lock lock = striped.get(key).readLock();
            lock.lock();
            try {
                return m.containsKey(key);
            } finally {
                lock.unlock();
            }
        }

        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException();
        }

        public V get(Object key) {
            Lock lock = striped.get(key).readLock();
            lock.lock();
            try {
                return m.get(key);
            } finally {
                lock.unlock();
            }
        }

        public V put(K key, V value) {
            Lock lock = striped.get(key).writeLock();
            lock.lock();
            try {
                return m.put(key, value);
            } finally {
                lock.unlock();
            }
        }

        public V remove(Object key) {
            Lock lock = striped.get(key).writeLock();
            lock.lock();
            try {
                return m.remove(key);
            } finally {
                lock.unlock();
            }
        }

        public void putAll(Map<? extends K, ? extends V> map) {
            m.putAll(map);
        }

        public void clear() {
            throw new UnsupportedOperationException();
        }

        public K firstKey() {
            return m.firstKey();
        }

        public K lastKey() {
            return m.lastKey();
        }

        public Cache<K, V> subCache(K from, boolean fromInclusive, K to, boolean toInclusive) {
            return m.subCache(from, fromInclusive, to, toInclusive);
        }

        public KeyValueIterator<K, V> range(K from, boolean fromInclusive, K to, boolean toInclusive) {
            return m.range(from, fromInclusive, to, toInclusive); // Must be manually synched by user!
        }

        public KeyValueIterator<K, V> all() {
            return m.all(); // Must be manually synched by user!
        }

        public Cache<K, V> descendingCache() {
            return m.descendingCache();
        }

        public void flush() {
            m.flush();
        }

        public synchronized void close() throws IOException {
            m.close();
        }

        public synchronized void destroy() throws IOException {
            m.destroy();
        }

        private transient Set<K> keySet;
        private transient Set<Map.Entry<K, V>> entrySet;
        private transient Collection<V> values;

        public Set<K> keySet() {
            return new ConcurrentSet<>(m.keySet(), striped);
        }

        public Set<Map.Entry<K, V>> entrySet() {
            return new ConcurrentSet<>(m.entrySet(), striped);
        }

        public Collection<V> values() {
            throw new UnsupportedOperationException();
        }

        public boolean equals(Object o) {
            if (this == o)
                return true;
            return m.equals(o);
        }

        public int hashCode() {
            return m.hashCode();
        }

        public String toString() {
            return m.toString();
        }

        // Override default methods in map
        @Override
        public V getOrDefault(Object k, V defaultValue) {
            Lock lock = striped.get(k).readLock();
            lock.lock();
            try {
                return m.getOrDefault(k, defaultValue);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void forEach(BiConsumer<? super K, ? super V> action) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
            throw new UnsupportedOperationException();
        }

        @Override
        public V putIfAbsent(K key, V value) {
            Lock lock = striped.get(key).writeLock();
            lock.lock();
            try {
                return m.putIfAbsent(key, value);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean remove(Object key, Object value) {
            Lock lock = striped.get(key).writeLock();
            lock.lock();
            try {
                return m.remove(key, value);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public boolean replace(K key, V oldValue, V newValue) {
            Lock lock = striped.get(key).writeLock();
            lock.lock();
            try {
                return m.replace(key, oldValue, newValue);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public V replace(K key, V value) {
            Lock lock = striped.get(key).writeLock();
            lock.lock();
            try {
                return m.replace(key, value);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public V computeIfAbsent(K key,
                                 Function<? super K, ? extends V> MappingFunction) {
            Lock lock = striped.get(key).writeLock();
            lock.lock();
            try {
                return m.computeIfAbsent(key, MappingFunction);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public V computeIfPresent(K key,
                                  BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
            Lock lock = striped.get(key).writeLock();
            lock.lock();
            try {
                return m.computeIfPresent(key, remappingFunction);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public V compute(K key,
                         BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
            Lock lock = striped.get(key).writeLock();
            lock.lock();
            try {
                return m.compute(key, remappingFunction);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public V merge(K key, V value,
                       BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
            Lock lock = striped.get(key).writeLock();
            lock.lock();
            try {
                return m.merge(key, value, remappingFunction);
            } finally {
                lock.unlock();
            }
        }
    }

    static class ConcurrentCollection<E> implements Collection<E>, Serializable {
        private static final long serialVersionUID = 3053995032091335093L;

        final Collection<E> c;  // Backing Collection
        final transient Striped<ReadWriteLock> striped;     // Object on which to synchronize

        ConcurrentCollection(Collection<E> c, Striped<ReadWriteLock> striped) {
            this.c = Objects.requireNonNull(c);
            this.striped = Objects.requireNonNull(striped);
        }

        public int size() {
            return c.size();
        }

        public boolean isEmpty() {
            return c.isEmpty();
        }

        public boolean contains(Object o) {
            Lock lock = striped.get(o).readLock();
            lock.lock();
            try {
                return c.contains(o);
            } finally {
                lock.unlock();
            }
        }

        public Object[] toArray() {
            throw new UnsupportedOperationException();
        }

        public <T> T[] toArray(T[] a) {
            throw new UnsupportedOperationException();
        }

        public Iterator<E> iterator() {
            return c.iterator(); // Must be manually synched by user!
        }

        public boolean add(E e) {
            Lock lock = striped.get(e).writeLock();
            lock.lock();
            try {
                return c.add(e);
            } finally {
                lock.unlock();
            }
        }

        public boolean remove(Object o) {
            Lock lock = striped.get(o).writeLock();
            lock.lock();
            try {
                return c.remove(o);
            } finally {
                lock.unlock();
            }
        }

        public boolean containsAll(Collection<?> coll) {
            for (Object o : coll) {
                if (!contains(o)) {
                    return false;
                }
            }
            return true;
        }

        public boolean addAll(Collection<? extends E> coll) {
            boolean changed = false;
            for (E e : coll) {
                changed |= c.add(e);
            }
            return changed;
        }

        public boolean removeAll(Collection<?> coll) {
            boolean changed = false;
            for (Object o : coll) {
                changed |= c.remove(o);
            }
            return changed;
        }

        public boolean retainAll(Collection<?> coll) {
            throw new UnsupportedOperationException();
        }

        public void clear() {
            throw new UnsupportedOperationException();
        }

        public String toString() {
            return c.toString();
        }

        // Override default methods in Collection
        @Override
        public void forEach(Consumer<? super E> consumer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeIf(Predicate<? super E> filter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Spliterator<E> spliterator() {
            return c.spliterator(); // Must be manually synched by user!
        }

        @Override
        public Stream<E> stream() {
            return c.stream(); // Must be manually synched by user!
        }

        @Override
        public Stream<E> parallelStream() {
            return c.parallelStream(); // Must be manually synched by user!
        }
    }

    static class ConcurrentSet<E>
        extends ConcurrentCollection<E>
        implements Set<E> {
        private static final long serialVersionUID = 487447009682186044L;

        ConcurrentSet(Set<E> s, Striped<ReadWriteLock> striped) {
            super(s, striped);
        }

        public boolean equals(Object o) {
            return c.equals(o);
        }

        public int hashCode() {
            return c.hashCode();
        }
    }

}

