package io.simplesource.kafka.spec;

import io.simplesource.api.Aggregator;
import io.simplesource.api.CommandHandler;
import io.simplesource.api.InitialValue;
import io.simplesource.api.InvalidSequenceHandler;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.api.AggregateSerdes;
import io.simplesource.kafka.api.ResourceNamingStrategy;
import lombok.Value;

import java.time.Duration;
import java.util.Map;


@Value
public final class AggregateSpec<K, C, E, A>  {

    private final String aggregateName;

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
