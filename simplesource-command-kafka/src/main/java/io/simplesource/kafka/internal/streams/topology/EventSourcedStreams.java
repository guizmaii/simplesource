package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandId;
import io.simplesource.data.Result;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.kafka.model.*;
import io.simplesource.kafka.spec.AggregateSpec;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.function.BiFunction;

final class EventSourcedStreams {

    static <K, C, E, A> KStream<K, CommandEvents<E, A>> getCommandEvents(
            AggregateSpec<K, C, E, A> ctx,
            final KStream<K, CommandRequest<K, C>> commandRequestStream,
            final KTable<K, AggregateUpdate<A>> aggregateTable) {
        return commandRequestStream.leftJoin(aggregateTable, (r, a) -> CommandRequestTransformer.getCommandEvents(ctx, a, r), ctx.commandRequestAggregateUpdateJoined());
    }

    static <K, E, A> KStream<K, ValueWithSequence<E>> getEventsWithSequence(final KStream<K, CommandEvents<E, A>> eventResultStream) {
        return eventResultStream.flatMapValues(result -> result.eventValue().fold(reasons -> Collections.emptyList(), ArrayList::new));
    }

    static <K, E, A> KStream<K, AggregateUpdateResult<A>> getAggregateUpdateResults(
            AggregateSpec<K, ?, E, A> ctx,
            final KStream<K, CommandEvents<E, A>> eventResultStream) {
        return eventResultStream
                .mapValues((serializedKey, result) -> {
                    final Result<CommandError, AggregateUpdate<A>> aggregateUpdateResult = result.eventValue().map(events -> {
                        final BiFunction<AggregateUpdate<A>, ValueWithSequence<E>, AggregateUpdate<A>> reducer =
                                (aggregateUpdate, eventWithSequence) -> new AggregateUpdate<>(
                                        ctx.aggregator().applyEvent(aggregateUpdate.aggregate(), eventWithSequence.value()),
                                        eventWithSequence.sequence()
                                );
                        return events.fold(
                                eventWithSequence -> new AggregateUpdate<>(
                                        ctx.aggregator().applyEvent(result.aggregate(), eventWithSequence.value()),
                                        eventWithSequence.sequence()
                                ),
                                reducer
                        );
                    });
                    return new AggregateUpdateResult<>(
                            result.commandId(),
                            result.readSequence(),
                            aggregateUpdateResult);
                });
    }

    static <K, A> KStream<K, AggregateUpdate<A>> getAggregateUpdates(final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream) {
        return aggregateUpdateStream
                .flatMapValues(update -> update.updatedAggregateResult().fold(
                        reasons -> Collections.emptyList(),
                        Collections::singletonList
                ));
    }

    static <K, A>  KStream<K, CommandResponse<K>> getCommandResponses(final KStream<K, AggregateUpdateResult<A>> aggregateUpdateStream) {
        return aggregateUpdateStream
                .mapValues((key, update) ->
                        CommandResponse.of(update.commandId(), key, update.readSequence(), update.updatedAggregateResult().map(AggregateUpdate::sequence))
                );
    }
}
