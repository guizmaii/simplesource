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

public final class EventSourcedTopology {

    public static <K, C, E, A> void addTopology(final AggregateSpec<K, C, E, A> ctx, final StreamsBuilder builder) {
        // Consume from topics
        final KStream<K, CommandRequest<K, C>> commandRequestStream = EventSourcedConsumer.commandRequestStream(ctx, builder);
        final KStream<K, CommandResponse<K>> commandResponseStream = EventSourcedConsumer.commandResponseStream(ctx, builder);
        final KTable<K, AggregateUpdate<A>> aggregateTable = EventSourcedConsumer.aggregateTable(ctx, builder);
        final DistributorContext<CommandId, CommandResponse<K>> distCtx = new DistributorContext<>(
            ctx.topicName(AggregateResources.TopicEntity.COMMAND_RESPONSE_TOPIC_MAP),
            new DistributorSerdes<>(ctx.serdes().commandId(), ctx.serdes().commandResponse()),
            ctx.generation().stateStoreSpec(),
            CommandResponse::commandId,
            CommandId::id
        );

        final KStream<CommandId, String> resultsTopicMapStream = builder.stream(distCtx.topicNameMapTopic, Consumed.with(distCtx.serdes().uuid(), Serdes.String()));

        // Handle idempotence by splitting stream into processed and unprocessed
        final Tuple2<KStream<K, CommandRequest<K, C>>, KStream<K, CommandResponse<K>>> reqResp =
            processedCommands(ctx, commandRequestStream, commandResponseStream);

        final KStream<K, CommandRequest<K, C>> unprocessedRequests = reqResp.v1();
        final KStream<K, CommandResponse<K>> processedResponses = reqResp.v2();

        // Transformations
        final KStream<K, CommandEvents<E, A>> commandEvents =
            unprocessedRequests.leftJoin(aggregateTable, (r, a) -> CommandRequestTransformer.getCommandEvents(ctx, a, r), ctx.commandRequestAggregateUpdateJoined());

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
        final val joinWindow = JoinWindows.of(distCtx.retention()).until(distCtx.retention().toMillis() * 2 + 1);
        final val joinWith = Joined.with(distCtx.serdes().uuid(), distCtx.serdes().value(), Serdes.String());

        commandResponseStream.selectKey((k, v) -> distCtx.idMapper.apply(v))
            .join(resultsTopicMapStream, Tuple2::of, joinWindow, joinWith)
            .map((uuid, tuple) -> KeyValue.pair(String.format("%s:%s", tuple.v2(), distCtx.keyToUuid.apply(uuid).toString()), tuple.v1()))
            .to((key, value, context) -> key.substring(0, key.length() - 37), Produced.with(Serdes.String(), distCtx.serdes().value()));
    }

    private static <K, C, E, A> Tuple2<KStream<K, CommandRequest<K, C>>, KStream<K, CommandResponse<K>>> processedCommands(
        final AggregateSpec<K, C, E, A> ctx,
        final KStream<K, CommandRequest<K, C>> commandRequestStream,
        final KStream<K, CommandResponse<K>> commandResponseStream
    ) {

        final KTable<CommandId, CommandResponse<K>> commandResponseById =
            commandResponseStream
                .selectKey((key, response) -> response.commandId())
                .groupByKey(ctx.serializedCommandResponse())
                .reduce((r1, r2) -> responseSequence(r1) > responseSequence(r2) ? r1 : r2);

        final KStream<K, Tuple2<CommandRequest<K, C>, CommandResponse<K>>> reqResp =
            commandRequestStream
                .selectKey((k, v) -> v.commandId())
                .leftJoin(commandResponseById, Tuple2::new, ctx.commandRequestResponseJoined())
                .selectKey((k, v) -> v.v1().aggregateKey());

        final KStream<K, Tuple2<CommandRequest<K, C>, CommandResponse<K>>>[] branches =
            reqResp.branch((k, tuple) -> tuple.v2() == null, (k, tuple) -> tuple.v2() != null);

        final KStream<K, CommandRequest<K, C>> unProcessed = branches[0].mapValues((k, tuple) -> tuple.v1());

        final KStream<K, CommandResponse<K>> processed = branches[1].mapValues((k, tuple) -> tuple.v2());

        return new Tuple2<>(unProcessed, processed);
    }

    private static <K> long responseSequence(final CommandResponse<K> response) {
        return response.sequenceResult().getOrElse(response.readSequence()).getSeq();
    }

}
