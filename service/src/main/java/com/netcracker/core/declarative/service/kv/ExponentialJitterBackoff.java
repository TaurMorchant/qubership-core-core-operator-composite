package com.netcracker.core.declarative.service.kv;

import java.time.Duration;
import java.util.Objects;
import java.util.Random;

public final class ExponentialJitterBackoff implements BackoffStrategy {
    private final Random rnd = new Random();

    @Override
    public Duration next(Duration current, Duration min, Duration max) {
        Objects.requireNonNull(min);
        Objects.requireNonNull(max);
        if (min.isNegative() || max.isNegative() || min.compareTo(max) > 0)
            throw new IllegalArgumentException("Invalid backoff bounds");

        Duration base = current.isZero() ? min : current.multipliedBy(2);
        if (base.compareTo(max) > 0) {
            base = max;
        }

        double factor = 1.0 + (rnd.nextDouble() * 0.30 - 0.15);
        long ms = Math.max(1, Math.round(base.toMillis() * factor));
        return Duration.ofMillis(ms);
    }
}
