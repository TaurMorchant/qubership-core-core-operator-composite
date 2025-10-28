package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.service.kv.KvLongPoller;

/**
 * Контракт на обработку полного состояния.
 * Бизнес-логика может быть любой (проверки консистентности, сохранение в БД, эмит событий и т.п.).
 */
public interface StructureStateHandler {

    /**
     * @param state     текущее полное состояние
     */
    void handle(CompositeStructureState state);
}
