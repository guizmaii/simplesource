package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.model.ValueWithSequence;
import io.simplesource.kafka.spec.AggregateSpec;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity.AGGREGATE;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.EVENT;

final class EventSourcedPublisher {
    static <K, C, E, A> void publishEvents(AggregateSpec<K, C, E, A> ctx, final KStream<K, ValueWithSequence<E>> eventStream) {
        eventStream.to(ctx.topicName(EVENT), Produced.with(ctx.serdes().aggregateKey(), ctx.serdes().valueWithSequence()));
    }

    static <K, A> void publishAggregateUpdates(AggregateSpec<K, ?, ?, A> ctx, final KStream<K, AggregateUpdate<A>> aggregateUpdateStream) {
        aggregateUpdateStream.to(ctx.topicName(AGGREGATE), Produced.with(ctx.serdes().aggregateKey(), ctx.serdes().aggregateUpdate()));
    }

    static <K> void publishCommandResponses(AggregateSpec<K, ?, ?, ?> ctx, final KStream<K, CommandResponse<K>> responseStream) {
        responseStream.to(ctx.topicName(AggregateResources.TopicEntity.COMMAND_RESPONSE), Produced.with(ctx.serdes().aggregateKey(), ctx.serdes().commandResponse()));
    }
}
