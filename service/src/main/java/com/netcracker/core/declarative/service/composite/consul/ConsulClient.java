package com.netcracker.core.declarative.service.composite.consul;

import com.netcracker.core.declarative.service.composite.consul.longpoll.PollResultHandler;

import java.time.Duration;

public interface ConsulClient {
    void awaitChanges(String path, long index, Duration wait, PollResultHandler handler);
}
