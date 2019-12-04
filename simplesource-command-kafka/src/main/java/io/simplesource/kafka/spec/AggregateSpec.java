package io.simplesource.kafka.spec;

import io.simplesource.api.*;
import io.simplesource.kafka.api.*;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import lombok.Value;
import lombok.val;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;

import java.util.Map;


@Value
public final class AggregateSpec<K, C, E, A>  {
    private final String aggregateName;
    private final Serialization<K, C, E, A> serialization;
    private final Generation<K, C, E, A> generation;
    private final Grouped<CommandId, CommandResponse<K>> serializedCommandResponse;
    private final Joined<CommandId, CommandRequest<K, C>, CommandResponse<K>> commandRequestResponseJoined;
    private final Joined<K, CommandRequest<K, C>, AggregateUpdate<A>> commandRequestAggregateUpdateJoined;

    public AggregateSpec(String aggregateName, Serialization<K, C, E, A> serialization, Generation<K, C, E, A> generation) {
        this.aggregateName = aggregateName;
        this.serialization = serialization;
        this.generation = generation;

        val serde = serialization.serdes();

        serializedCommandResponse = Grouped.with(serde.commandId(), serde.commandResponse());
        commandRequestResponseJoined = Joined.with(serde.commandId(), serde.commandRequest(), serde.commandResponse());
        commandRequestAggregateUpdateJoined = Joined.with(serde.aggregateKey(), serde.commandRequest(), serde.aggregateUpdate());
    }

    public AggregateSerdes<K, C, E, A> serdes() {
        return serialization.serdes();
    }

    public String topicName(AggregateResources.TopicEntity entity) {
        return serialization.resourceNamingStrategy().topicName(aggregateName, entity.name());
    }

    public InitialValue<K, A> initialValue() {
        return generation.initialValue();
    }

    public Aggregator<E, A> aggregator() {
        return generation.aggregator();
    }

    @Value
    public static class Serialization<K, C, E, A> {
        private final ResourceNamingStrategy resourceNamingStrategy;
        private final AggregateSerdes<K, C, E, A> serdes;
    }

    @Value
    public static class Generation<K, C, E, A> {
        private final Map<AggregateResources.TopicEntity, TopicSpec> topicConfig;
        private final WindowSpec stateStoreSpec;
        private final CommandHandler<K, C, E, A> commandHandler;
        private final InvalidSequenceHandler<K, C, A> invalidSequenceHandler;
        private final Aggregator<E, A> aggregator;
        private final InitialValue<K, A> initialValue;
    }
}
