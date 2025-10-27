package com.netcracker.core.declarative.service.kv;

import lombok.Builder;
import lombok.Value;

import java.time.Duration;

@Value
@Builder
public class KvPollConfig {
    @Builder.Default Duration wait = Duration.ofSeconds(9);
    @Builder.Default Duration backoffMin = Duration.ofSeconds(1);
    @Builder.Default Duration backoffMax = Duration.ofSeconds(30);
    @Builder.Default Duration initialDelay = Duration.ZERO;
    @Builder.Default boolean fireOnFirstSuccess = true;
}
