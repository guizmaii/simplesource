package io.simplesource.kafka.internal.client;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * ExpiringMap is a Map type that allows you to
 * 1. Create a map entry at a particular time
 * 1. Modify any existing map entries as long as they are present
 * 2. Expire all map entries at a certain point, calling the supplied cleanup function
 *
 * @param <K> Key type
 * @param <V> Value type
 */
final class ExpiringMap<K, V> {

    private final ConcurrentHashMap<Long, ConcurrentHashMap<K, V>> outerMap = new ConcurrentHashMap<>();
    private final Duration retention;
    private final Clock clock;

    ExpiringMap(final Duration retention, final Clock clock) {
        this.retention = retention;
        this.clock = clock;
    }

    final void insertIfAbsent(final K k, final Supplier<V> lazyV) {
        final long outerKey = Instant.now(clock).getEpochSecond() / retention.getSeconds();
        final ConcurrentHashMap<K, V> innerMap = outerMap.computeIfAbsent(outerKey, oKey -> new ConcurrentHashMap<>());
        innerMap.computeIfAbsent(k, ik -> lazyV.get());
    }

    final V computeIfPresent(final K k, final Function<V, V> vToV) {
        for (final ConcurrentHashMap<K, V> inner: outerMap.values()) {
            final V newV = inner.computeIfPresent(k, (ik, v) -> vToV.apply(v));
            if (newV != null)
                return newV;
        }
        return null;
    }

    final void removeStaleAsync(final Consumer<V> consumeV)  {
        if (outerMap.size() < 3) return;
        new Thread(() -> {
            long outerKey = Instant.now(clock).getEpochSecond() / retention.getSeconds();
            removeIf(consumeV, k -> k + 1 < outerKey);
        }).start();
    }

    final void removeAll(final Consumer<V> consumeV)  {
        removeIf(consumeV, k -> true);
    }

    private void removeIf(final Consumer<V> consumeV, final Predicate<Long> outerKeyCondition) {
        outerMap.keySet().forEach(k -> {
            if (outerKeyCondition.test(k)) {
                outerMap.values().forEach(innerMap -> {
                    innerMap.values().forEach(consumeV);
                });
                outerMap.remove(k);
            }
        });
    }

}
