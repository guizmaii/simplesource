package io.simplesource.kafka.dsl;

import io.simplesource.api.CommandAPI;
import io.simplesource.kafka.internal.client.KafkaCommandAPI;
import io.simplesource.kafka.internal.streams.EventSourcedStreamsApp;
import io.simplesource.kafka.internal.util.NamedThreadFactory;
import io.simplesource.kafka.spec.AggregateSetSpec;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.CommandSpec;
import io.simplesource.kafka.util.SpecUtils;
import lombok.val;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class EventSourcedApp {
    private KafkaConfig kafkaConfig;
    private Map<String, AggregateSpec<?, ?, ?, ?>> aggregateConfigMap = new HashMap<>();
    private AggregateSetSpec aggregateSetSpec;
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("EventSourcedApp-scheduler"));

    public static final class EventSourcedAppBuilder {
        EventSourcedApp app = new EventSourcedApp();

        public EventSourcedAppBuilder withKafkaConfig(
                final Function<KafkaConfig.Builder, KafkaConfig> builder) {
            app.kafkaConfig = builder.apply(new KafkaConfig.Builder());
            return this;
        }

        public EventSourcedAppBuilder withKafkaConfig(final KafkaConfig kafkaConfig) {
            app.kafkaConfig = kafkaConfig;
            return this;
        }

        public EventSourcedAppBuilder withScheduler(final ScheduledExecutorService scheduler) {
            app.scheduler = scheduler;
            return this;
        }

        public <K, C, E, A> EventSourcedAppBuilder addAggregate(
                final Function<AggregateBuilder<K, C, E, A>, AggregateSpec<K, C, E, A>> buildSteps) {
            AggregateBuilder<K, C, E, A> builder = AggregateBuilder.newBuilder();
            final AggregateSpec<K, C, E, A> spec = buildSteps.apply(builder);
            app.aggregateConfigMap.put(spec.aggregateName(), spec);
            return this;
        }

        public <K, C, E, A> EventSourcedAppBuilder addAggregate(final AggregateSpec<K, C, E, A> spec) {
            app.aggregateConfigMap.put(spec.aggregateName(), spec);
            return this;
        }

        public EventSourcedApp start() {
            requireNonNull(app.kafkaConfig, "KafkaConfig has not been defined. Please define it with 'withKafkaConfig' method.");

            final AggregateSetSpec aggregateSetSpec = new AggregateSetSpec(
                    app.kafkaConfig,
                    app.aggregateConfigMap);

            new EventSourcedStreamsApp(aggregateSetSpec).start();

            app.aggregateSetSpec = aggregateSetSpec;
            return app;
        }
    }

    public static final class UnknownAggregateException extends RuntimeException {
        final String aggregateName;
        final Class k;
        final Class c;

        public UnknownAggregateException(final String aggregateName, final Class k, final Class c) {
            super(aggregateName + " doesn't exists for the types <" + k.getSimpleName() + ", " + c.getSimpleName() + ">");
            this.aggregateName = aggregateName;
            this.k = k;
            this.c = c;
        }
    }

    /**
     * Creates a CommandAPI instance
     *
     * Used for directly exposing a CommandAPI from within a Simple Sourcing application
     * If creating a CommandAPI from an external application, rather use the CommandAPIBuilder DSL
     *
     * @return a CommandAPI
     */
public <K, C> CommandAPI<K, C> createCommandAPI(String clientId, String aggregateName) throws UnknownAggregateException  {
        try {
            AggregateSpec<K, C, ?, ?> aggregateSpec = (AggregateSpec<K, C, ?, ?>) aggregateSetSpec.aggregateConfigMap().get(aggregateName);
            CommandSpec<K, C> commandSpec = SpecUtils.getCommandSpec(aggregateSpec, clientId);
            return new KafkaCommandAPI<>(commandSpec, kafkaConfig, scheduler);
        } catch (ClassCastException e) {
            // Hack found here: https://stackoverflow.com/questions/3437897/how-to-get-a-class-instance-of-generics-type-t
            val actualTypeArguments = ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments();
            Class<K> k = (Class<K>) actualTypeArguments[0];
            Class<C> c = (Class<C>) actualTypeArguments[1];

            throw new UnknownAggregateException(aggregateName, k, c);
        }
    }
}
