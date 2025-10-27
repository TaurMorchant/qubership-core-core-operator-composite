package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.service.kv.KvLongPoller;

/**
 * Контракт на обработку полного состояния.
 * Бизнес-логика может быть любой (проверки консистентности, сохранение в БД, эмит событий и т.п.).
 */
public interface StructureStateHandler {

    /**
     * @param state     текущее полное состояние
     * @param idx       (prev, now) индексы Consul
     * @param initial   true — первый успешный снимок после (ре)старта/переключения префикса
     */
    void handle(StructureState state, KvLongPoller.IndexPair idx, boolean initial);
}
