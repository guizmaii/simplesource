package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.kafka.model.AggregateUpdate;
import io.simplesource.kafka.model.CommandRequest;
import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.kafka.spec.AggregateSpec;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity.AGGREGATE;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.COMMAND_REQUEST;
import static io.simplesource.kafka.api.AggregateResources.TopicEntity.COMMAND_RESPONSE;

final class EventSourcedConsumer {

    static <K, C> KStream<K, CommandRequest<K, C>> commandRequestStream(AggregateSpec<K, C, ?, ?> ctx, final StreamsBuilder builder) {
        return builder.stream(ctx.topicName(COMMAND_REQUEST), Consumed.with(ctx.serdes().aggregateKey(), ctx.serdes().commandRequest()));
    }

    static <K, A> KTable<K, AggregateUpdate<A>> aggregateTable(AggregateSpec<K, ?, ?, A> ctx, final StreamsBuilder builder) {
        return builder.table(ctx.topicName(AGGREGATE), Consumed.with(ctx.serdes().aggregateKey(), ctx.serdes().aggregateUpdate()));
    }

    static <K, C> KStream<K, CommandResponse<K>> commandResponseStream(AggregateSpec<K, C, ?, ?> ctx, final StreamsBuilder builder) {
        return builder.stream(ctx.topicName(COMMAND_RESPONSE), Consumed.with(ctx.serdes().aggregateKey(), ctx.serdes().commandResponse()));
    }
}

