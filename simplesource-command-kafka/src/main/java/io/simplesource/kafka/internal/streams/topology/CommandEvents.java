package io.simplesource.kafka.internal.streams.topology;

import io.simplesource.api.CommandError;
import io.simplesource.api.CommandId;
import io.simplesource.data.Sequence;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.kafka.model.ValueWithSequence;
import lombok.AllArgsConstructor;
import lombok.Value;

/**
 * @param <A> the aggregate aggregate_update
 * @param <E> all events generated for this aggregate
 */
@Value
@AllArgsConstructor
final class CommandEvents<A, E> {
    private final CommandId commandId;
    private final Sequence readSequence;
    private final A aggregate;
    private final Result<CommandError, NonEmptyList<ValueWithSequence<E>>> eventValue;
}
