package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandId;
import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.kafka.model.*;
import io.simplesource.kafka.spec.AggregateSpec;
import lombok.Value;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

public final class EventSourcedTopology {

    @Value
    static final class InputStreams<K, C> {
        public final KStream<K, CommandRequest<K, C>> commandRequest;
        public final KStream<K, CommandResponse<K>> commandResponse;
    }

    public static <K, C, E, A> InputStreams<K, C> addTopology(AggregateSpec<K, C, E, A> ctx, final StreamsBuilder builder) {
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
        Tuple2<KStream<K, CommandRequest<K, C>>, KStream<K, CommandResponse<K>>> reqResp =
                processedCommands(ctx, commandRequestStream, commandResponseStream);

        final KStream<K, CommandRequest<K, C>> unprocessedRequests = reqResp.v1();
        final KStream<K, CommandResponse<K>> processedResponses = reqResp.v2();
        
        // Transformations
        final KStream<K, CommandEvents<E, A>> commandEvents = EventSourcedStreams.getCommandEvents(ctx, unprocessedRequests, aggregateTable);
        final KStream<K, ValueWithSequence<E>> eventsWithSequence = EventSourcedStreams.getEventsWithSequence(commandEvents);

        final KStream<K, AggregateUpdateResult<A>> aggregateUpdateResults = EventSourcedStreams.getAggregateUpdateResults(ctx, commandEvents);
        final KStream<K, AggregateUpdate<A>> aggregateUpdates = EventSourcedStreams.getAggregateUpdates(aggregateUpdateResults);
        final KStream<K, CommandResponse<K>>commandResponses = EventSourcedStreams.getCommandResponses(aggregateUpdateResults);

        // Produce to topics
        EventSourcedPublisher.publishEvents(ctx, eventsWithSequence);
        EventSourcedPublisher.publishAggregateUpdates(ctx, aggregateUpdates);
        EventSourcedPublisher.publishCommandResponses(ctx, processedResponses);
        EventSourcedPublisher.publishCommandResponses(ctx, commandResponses);

        // Distribute command results
        ResultDistributor.distribute(distCtx, commandResponseStream, resultsTopicMapStream);

        // return input streams
        return new InputStreams<>(commandRequestStream, commandResponseStream);
    }


    private static  <K, C, E, A> Tuple2<KStream<K, CommandRequest<K, C>>, KStream<K, CommandResponse<K>>> processedCommands(
            AggregateSpec<K, C, E, A> ctx,
            final KStream<K, CommandRequest<K, C>> commandRequestStream,
            final KStream<K, CommandResponse<K>> commandResponseStream) {

        final KTable<CommandId, CommandResponse<K>> commandResponseById = commandResponseStream
                .selectKey((key, response) -> response.commandId())
                .groupByKey(ctx.serializedCommandResponse())
                .reduce((r1, r2) -> responseSequence(r1) > responseSequence(r2) ? r1 : r2);

        final KStream<K, Tuple2<CommandRequest<K, C>, CommandResponse<K>>> reqResp = commandRequestStream
                .selectKey((k, v) -> v.commandId())
                .leftJoin(commandResponseById, Tuple2::new, ctx.commandRequestResponseJoined())
                .selectKey((k, v) -> v.v1().aggregateKey());

        KStream<K, Tuple2<CommandRequest<K, C>, CommandResponse<K>>>[] branches =
                reqResp.branch((k, tuple) -> tuple.v2() == null, (k, tuple) -> tuple.v2() != null);

        KStream<K, CommandRequest<K, C>> unProcessed = branches[0].mapValues((k, tuple) -> tuple.v1());

        KStream<K, CommandResponse<K>> processed = branches[1].mapValues((k, tuple) -> tuple.v2());

        return new Tuple2<>(unProcessed, processed);
    }

    private static <K> long responseSequence(CommandResponse<K> response) {
        return response.sequenceResult().getOrElse(response.readSequence()).getSeq();
    }

}


