package io.simplesource.kafka.util;

import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.CommandSpec;

public class SpecUtils {
    public static <K, C> CommandSpec<K, C> getCommandSpec(final AggregateSpec<K, C, ?, ?> aSpec, final String clientId) {
        return new CommandSpec<>(
                aSpec.aggregateName(),
                clientId,
                aSpec.resourceNamingStrategy(),
                aSpec.serdes(),
                aSpec.generation().stateStoreSpec(),
                aSpec.topicConfig(AggregateResources.TopicEntity.COMMAND_RESPONSE));
    }
}
