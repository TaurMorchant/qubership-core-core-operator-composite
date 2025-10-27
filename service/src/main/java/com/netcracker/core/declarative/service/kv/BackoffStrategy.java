package com.netcracker.core.declarative.service.kv;

import java.time.Duration;

public interface BackoffStrategy {
    Duration next(Duration current, Duration min, Duration max);
}