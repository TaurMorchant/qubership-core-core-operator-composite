package com.netcracker.core.declarative.service.composite.consul;

import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;

public interface ConsulSnapshotHandler {

    void handle(ConsulPrefixSnapshot state);
}
