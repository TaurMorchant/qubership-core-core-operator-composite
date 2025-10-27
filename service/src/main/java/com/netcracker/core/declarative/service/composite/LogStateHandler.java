package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.service.kv.KvLongPoller;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogStateHandler implements StructureStateHandler {
    @Override
    public void handle(StructureState state, KvLongPoller.IndexPair idx, boolean initial) {
        log.info("VLLA HANDLE CHANGE {}", state.data());
    }
}
