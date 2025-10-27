package com.netcracker.core.declarative.service.composite;

import java.util.Map;

/**
 * Полное состояние структуры, основанное на текущем снимке KV.
 * Содержит минимально необходимое представление для бизнес-логики.
 */
public record StructureState(String prefix, Map<String, String> data) {}
