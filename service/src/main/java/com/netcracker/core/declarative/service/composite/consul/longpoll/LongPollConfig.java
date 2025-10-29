package com.netcracker.core.declarative.service.composite.consul.longpoll;

import lombok.Builder;
import lombok.Value;

import java.time.Duration;

@Value
@Builder
public class LongPollConfig {
    @Builder.Default Duration wait = Duration.ofMinutes(9);
    @Builder.Default boolean fireOnFirstSuccess = true;
}
