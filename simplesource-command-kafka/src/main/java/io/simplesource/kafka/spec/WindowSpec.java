package io.simplesource.kafka.spec;

import lombok.Value;

import java.time.Duration;

@Value
public final class WindowSpec {
    private final Duration retention;
}
