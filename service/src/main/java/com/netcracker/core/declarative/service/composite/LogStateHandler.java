package com.netcracker.core.declarative.service.composite;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogStateHandler implements StructureStateHandler {
    @Override
    public void handle(CompositeStructureState state) {
        log.info("VLLA HANDLE CHANGE {}", state.data());
    }
}
