package com.netcracker.core.declarative.service;

import io.vertx.ext.consul.BlockingQueryOptions;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Slf4j
public final class CompositeWatcher implements AutoCloseable {

    private final ConsulClient client;
    private final String key; // точный путь ключа, БЕЗ завершающего "/"
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // состояние для фильтрации "настоящих" изменений
    private volatile Long lastHeaderIndex = null; // X-Consul-Index из заголовка ответа
    private volatile String lastValue = null;     // предыдущее значение ключа (String)
    private volatile boolean lastExists = false;  // существовал ли ключ

    public CompositeWatcher(ConsulClient client, String key) {
        this(client, key, Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "consul-key-watcher");
            t.setDaemon(true);
            return t;
        }));
    }

    public CompositeWatcher(ConsulClient client, String key, ScheduledExecutorService scheduler) {
        this.client = Objects.requireNonNull(client, "client");
        if (key == null || key.isBlank() || key.endsWith("/")) {
            throw new IllegalArgumentException("key must be a non-empty path without trailing slash");
        }
        this.key = key;
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    /**
     * onChange будет вызван только когда значение (или факт существования) ключа изменится.
     * В handler передаем снимок в формате KeyValueList (либо пустой список, если ключ отсутствует).
     */
    public void start(Consumer<KeyValueList> onChange) {
        Objects.requireNonNull(onChange, "onChange");
        if (!running.compareAndSet(false, true)) return;
        log.info("Starting Consul key watcher for '{}'", key);
        // первый запрос с index=0 — получаем актуальный индекс и текущее состояние
        loop(onChange, 0L, 0L);
    }

    private void loop(Consumer<KeyValueList> onChange, Long indexForWait, long backoffMs) {
        if (!running.get()) return;

        final BlockingQueryOptions opts = new BlockingQueryOptions()
                .setWait("10m")                     // блокируем до 10 минут
                .setIndex(indexForWait == null ? 0L : indexForWait); // никогда не передаем null

        client.getValuesWithOptions(key, opts, ar -> {
            if (!running.get()) return;

            if (ar.failed()) {
                long next = nextBackoff(backoffMs);
                log.warn("Consul watch error on '{}': {} (retry in {} ms)", key, ar.cause().toString(), next);
                scheduler.schedule(() -> loop(onChange, indexForWait, next), next, TimeUnit.MILLISECONDS);
                return;
            }

            KeyValueList kvs = ar.result();
            Long hdr = kvs.getIndex(); // X-Consul-Index с сервера

            log.debug("VLLA KeyValueList = {}", kvs.getList());

            // Если индекс не изменился — это либо таймаут wait, либо "дрожь" сети. Просто продолжаем ждать.
            if (lastHeaderIndex != null && hdr.equals(lastHeaderIndex)) {
                scheduler.execute(() -> loop(onChange, lastHeaderIndex, 0L));
                return;
            }

            // Разбираем текущее значение ключа (может отсутствовать)
            KeyValue current = firstOrNull(kvs);
            boolean existsNow = current != null;
            String valueNow = existsNow ? current.getValue() : null;

            boolean changed = (existsNow != lastExists)
                              || (existsNow && !Objects.equals(valueNow, lastValue));

            // Обновляем локальное состояние
            lastHeaderIndex = hdr;
            lastExists = existsNow;
            lastValue = valueNow;

            if (changed) {
                try {
                    onChange.accept(kvs); // отдаём текущий снимок (пустой список = ключ отсутствует)
                } catch (Throwable t) {
                    log.error("Error in onChange handler for key '{}'", key, t);
                }
            }

            // Продолжаем ждать следующего изменения, теперь блокируемся на новом индексе
            scheduler.execute(() -> loop(onChange, lastHeaderIndex, 0L));
        });
    }

    private static KeyValue firstOrNull(KeyValueList list) {
        if (list == null || list.getList() == null || list.getList().isEmpty()) return null;
        // Consul /v1/kv/<key> возвращает массив, берём первую запись
        return list.getList().get(0);
    }

    private static long nextBackoff(long prev) {
        if (prev <= 0) return 500L;
        long next = prev * 2;
        return Math.min(next, 5_000L);
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;
        log.info("Stopping Consul key watcher for '{}'", key);
        scheduler.shutdownNow();
        try {
            client.close();
        } catch (Exception ignore) {
        }
    }
}
