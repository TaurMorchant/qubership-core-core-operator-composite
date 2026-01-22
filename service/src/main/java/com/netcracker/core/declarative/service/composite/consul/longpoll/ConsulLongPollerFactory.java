package com.netcracker.core.declarative.service.composite.consul.longpoll;

import com.netcracker.core.declarative.service.composite.consul.ConsulClient;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;

import java.util.function.Consumer;

/**
 * Factory for creating {@link ConsulLongPoller} instances.
 */
@FunctionalInterface
public interface ConsulLongPollerFactory {

    ConsulLongPoller create(String path,
                            ConsulClient consulClient,
                            LongPollConfig pollConfig,
                            Consumer<ConsulPrefixSnapshot> onSnapshot);

    static ConsulLongPollerFactory defaultFactory() {
        return (path, consulClient, pollConfig, onSnapshot) -> ConsulLongPoller.builder()
                .path(path)
                .consulClient(consulClient)
                .pollConfig(pollConfig)
                .onSnapshot(onSnapshot)
                .build();
    }
}
