package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.spec.WindowSpec;
import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;

@Value
final class DistributorSerdes<K, V> {
    Serde<K> uuid;
    Serde<V> value;
}

@Value
final class DistributorContext<K, V> {
    public final String topicNameMapTopic;
    public final DistributorSerdes<K, V> serdes;
    private final WindowSpec responseWindowSpec;
    public final Function<V, K> idMapper;
    public final Function<K, UUID> keyToUuid;

    public Duration retention() {
        return responseWindowSpec.retention();
    }
}
