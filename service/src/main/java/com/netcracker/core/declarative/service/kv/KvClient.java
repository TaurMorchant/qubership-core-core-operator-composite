package com.netcracker.core.declarative.service.kv;

import io.vertx.ext.consul.KeyValueList;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Тонкая абстракция над источником KV (Consul).
 * Позволяет подменять реализацию (Vert.x, mock) без изменения поллера.
 */
public interface KvClient {

    /**
     * Асинхронно получить значения по пути с блокирующими опциями index/wait.
     *
     * @param path     ключ или префикс
     * @param index    предыдущий индекс (0 для первого запроса)
     * @param wait     желаемый timeout long-poll
     * @param handler  (result, error) колбэк
     */
    void getValues(String path, long index, Duration wait,
                   BiConsumer<KeyValueList, Throwable> handler);

    /** Утилита для обёрток handler’а. */
    static BiConsumer<KeyValueList, Throwable> safe(BiConsumer<KeyValueList, Throwable> h) {
        return Objects.requireNonNull(h);
    }
}
