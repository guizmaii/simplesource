package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandId;
import io.simplesource.data.Result;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.kafka.model.*;
import io.simplesource.kafka.spec.AggregateSpec;
import lombok.val;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.Collections;

import static io.simplesource.kafka.api.AggregateResources.TopicEntity.*;

public final class EventSourcedTopology {

    public static <K, C, E, A> void addTopology(final AggregateSpec<K, C, E, A> ctx, final StreamsBuilder builder) {
        // Consume from topics
        final KStream<K, CommandRequest<K, C>> commandRequestStream =
            builder.stream(ctx.topicName(COMMAND_REQUEST), Consumed.with(ctx.serdes().aggregateKey(), ctx.serdes().commandRequest()));

        final KStream<K, CommandResponse<K>> commandResponseStream =
            builder.stream(ctx.topicName(COMMAND_RESPONSE), Consumed.with(ctx.serdes().aggregateKey(), ctx.serdes().commandResponse()));

        final KTable<K, AggregateUpdate<A>> aggregateTable =
            builder.table(ctx.topicName(AGGREGATE), Consumed.with(ctx.serdes().aggregateKey(), ctx.serdes().aggregateUpdate()));

        final KStream<CommandId, String> resultsTopicMapStream =
            builder.stream(ctx.topicName(COMMAND_RESPONSE_TOPIC_MAP), Consumed.with(ctx.serdes().commandId(), Serdes.String()));

        // Handle idempotence by splitting stream into processed and unprocessed

        final KTable<CommandId, CommandResponse<K>> commandResponseById =
            commandResponseStream
                .selectKey((key, response) -> response.commandId())
                .groupByKey(Grouped.with(ctx.serdes().commandId(), ctx.serdes().commandResponse()))
                .reduce((r1, r2) -> responseSequence(r1) > responseSequence(r2) ? r1 : r2);

        final val requestCommandResponseJoined = Joined.with(ctx.serdes().commandId(), ctx.serdes().commandRequest(), ctx.serdes().commandResponse());
        final KStream<K, Tuple2<CommandRequest<K, C>, CommandResponse<K>>>[] branches =
            commandRequestStream
                .selectKey((k, v) -> v.commandId())
                .leftJoin(commandResponseById, Tuple2::new, requestCommandResponseJoined)
                .selectKey((k, v) -> v.v1().aggregateKey())
                .branch((k, tuple) -> tuple.v2() == null, (k, tuple) -> tuple.v2() != null);

        final KStream<K, CommandRequest<K, C>> unprocessedRequests = branches[0].mapValues((k, tuple) -> tuple.v1());
        final KStream<K, CommandResponse<K>> processedResponses = branches[1].mapValues((k, tuple) -> tuple.v2());

        // Transformations
        final val commandRequestAggregateUpdateJoined = Joined.with(ctx.serdes().aggregateKey(), ctx.serdes().commandRequest(), ctx.serdes().aggregateUpdate());
        final KStream<K, CommandEvents<E, A>> commandEvents =
            unprocessedRequests.leftJoin(aggregateTable, (r, a) -> CommandRequestTransformer.getCommandEvents(ctx, a, r), commandRequestAggregateUpdateJoined);

        final KStream<K, ValueWithSequence<E>> eventsWithSequence =
            commandEvents.flatMapValues(result -> result.eventValue().fold(reasons -> Collections.emptyList(), ArrayList::new));

        final KStream<K, AggregateUpdateResult<A>> aggregateUpdateResults =
            commandEvents
                .mapValues((serializedKey, result) -> {
                    final Result<CommandError, AggregateUpdate<A>> aggregateUpdateResult =
                        result.eventValue().map(events ->
                            events.fold(
                                eventWithSequence -> new AggregateUpdate<>(
                                    ctx.aggregator().applyEvent(result.aggregate(), eventWithSequence.value()),
                                    eventWithSequence.sequence()
                                ),
                                (aggregateUpdate, eventWithSequence) -> new AggregateUpdate<>(
                                    ctx.aggregator().applyEvent(aggregateUpdate.aggregate(), eventWithSequence.value()),
                                    eventWithSequence.sequence()
                                )
                            ));

                    return new AggregateUpdateResult<>(result.commandId(), result.readSequence(), aggregateUpdateResult);
                });

        final KStream<K, AggregateUpdate<A>> aggregateUpdates =
            aggregateUpdateResults
                .flatMapValues(update ->
                    update.updatedAggregateResult().fold(reasons -> Collections.emptyList(), Collections::singletonList)
                );

        final KStream<K, CommandResponse<K>> commandResponses =
            aggregateUpdateResults
                .mapValues((key, update) ->
                    CommandResponse.of(update.commandId(), key, update.readSequence(), update.updatedAggregateResult().map(AggregateUpdate::sequence))
                );

        // Produce to topics
        EventSourcedPublisher.publishEvents(ctx, eventsWithSequence);
        EventSourcedPublisher.publishAggregateUpdates(ctx, aggregateUpdates);
        EventSourcedPublisher.publishCommandResponses(ctx, processedResponses);
        EventSourcedPublisher.publishCommandResponses(ctx, commandResponses);

        // Distribute command results
        final val joinWindow = JoinWindows.of(ctx.retention()).until(ctx.retention().toMillis() * 2 + 1);
        final val joinWith = Joined.with(ctx.serdes().commandId(), ctx.serdes().commandResponse(), Serdes.String());

        commandResponseStream
            .selectKey((k, v) -> v.commandId())
            .join(resultsTopicMapStream, Tuple2::new, joinWindow, joinWith)
            .map((commandId, tuple) -> KeyValue.pair(String.format("%s:%s", tuple.v2(), commandId.id.toString()), tuple.v1()))
            .to((key, value, context) -> key.substring(0, key.length() - 37), Produced.with(Serdes.String(), ctx.serdes().commandResponse()));
    }

    private static <K> long responseSequence(final CommandResponse<K> response) {
        return response.sequenceResult().getOrElse(response.readSequence()).getSeq();
    }

}
