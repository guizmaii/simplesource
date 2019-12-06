package io.simplesource.kafka.client;


import io.simplesource.api.CommandAPI;
import io.simplesource.kafka.dsl.KafkaConfig;
import io.simplesource.kafka.internal.client.KafkaCommandAPI;
import io.simplesource.kafka.spec.CommandSpec;
import monix.execution.Scheduler;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class EventSourcedClient {
    private KafkaConfig kafkaConfig;
    private Scheduler scheduler;

    public EventSourcedClient withKafkaConfig(final Function<? super KafkaConfig.Builder, KafkaConfig> builderFn) {
        kafkaConfig = builderFn.apply(new KafkaConfig.Builder(true));
        return this;
    }

    public EventSourcedClient withScheduler(final Scheduler scheduler) {
        this.scheduler = scheduler;
        return this;
    }

    public <K, C> CommandAPI<K, C> createCommandAPI(final Function<CommandAPIBuilder<K, C>, CommandSpec<K, C>> buildSteps) {
        return createCommandAPI(buildSteps.apply(CommandAPIBuilder.newBuilder()));
    }

    public <K, C> CommandAPI<K, C> createCommandAPI(final CommandSpec<K, C> commandSpec) {
        requireNonNull(scheduler, "Scheduler has not been defined. Please define with with 'withScheduler' method.");

        return new KafkaCommandAPI<>(commandSpec, kafkaConfig, scheduler);
    }
}
