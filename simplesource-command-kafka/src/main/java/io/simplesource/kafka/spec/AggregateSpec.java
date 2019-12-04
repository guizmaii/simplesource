package io.simplesource.kafka.spec;

import io.simplesource.api.*;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import lombok.Value;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;

import java.time.Duration;
import java.util.Map;


@Value
public final class AggregateSpec<K, C, E, A>  {

    private final String aggregateName;
    private final Grouped<CommandId, CommandResponse<K>> serializedCommandResponse;
    private final Joined<CommandId, CommandRequest<K, C>, CommandResponse<K>> commandRequestResponseJoined;
    private final Joined<K, CommandRequest<K, C>, AggregateUpdate<A>> commandRequestAggregateUpdateJoined;

    private final ResourceNamingStrategy resourceNamingStrategy;
    private final AggregateSerdes<K, C, E, A> serdes;

    private final Map<AggregateResources.TopicEntity, TopicSpec> topicConfig;
    private final WindowSpec stateStoreSpec;
    private final CommandHandler<K, C, E, A> commandHandler;
    private final InvalidSequenceHandler<K, C, A> invalidSequenceHandler;
    private final Aggregator<E, A> aggregator;
    private final InitialValue<K, A> initialValue;

    public AggregateSpec(
        final String aggregateName,
        final ResourceNamingStrategy resourceNamingStrategy,
        final AggregateSerdes<K, C, E, A> serdes,
        final Map<AggregateResources.TopicEntity, TopicSpec> topicConfig,
        final WindowSpec stateStoreSpec,
        final CommandHandler<K, C, E, A> commandHandler,
        final InvalidSequenceHandler<K, C, A> invalidSequenceHandler,
        final Aggregator<E, A> aggregator,
        final InitialValue<K, A> initialValue
    ) {
        this.aggregateName = aggregateName;
        this.resourceNamingStrategy = resourceNamingStrategy;
        this.serdes = serdes;
        this.topicConfig = topicConfig;
        this.stateStoreSpec = stateStoreSpec;
        this.commandHandler = commandHandler;
        this.invalidSequenceHandler = invalidSequenceHandler;
        this.aggregator = aggregator;
        this.initialValue = initialValue;

        serializedCommandResponse = Grouped.with(serdes.commandId(), serdes.commandResponse());
        commandRequestResponseJoined = Joined.with(serdes.commandId(), serdes.commandRequest(), serdes.commandResponse());
        commandRequestAggregateUpdateJoined = Joined.with(serdes.aggregateKey(), serdes.commandRequest(), serdes.aggregateUpdate());
    }

    public String topicName(final AggregateResources.TopicEntity entity) {
        return resourceNamingStrategy.topicName(aggregateName, entity.name());
    }

    public TopicSpec topicConfig(final AggregateResources.TopicEntity entity) {
        return topicConfig.get(entity);
    }

    public Duration retention() {
        return stateStoreSpec.retention();
    }

}
