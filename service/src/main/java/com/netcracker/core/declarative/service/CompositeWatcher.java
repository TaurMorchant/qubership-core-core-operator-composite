package com.netcracker.core.declarative.service;

import io.vertx.ext.consul.BlockingQueryOptions;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueList;
import org.jboss.logging.Logger;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public final class CompositeWatcher implements AutoCloseable {
    private static final Logger log = Logger.getLogger(CompositeWatcher.class);

    private final ConsulClient client;
    private final String prefix; // должен заканчиваться на '/'
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public CompositeWatcher(ConsulClient client, String prefix) {
        this(client, prefix, Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "consul-watcher");
            t.setDaemon(true);
            return t;
        }));
    }

    public CompositeWatcher(ConsulClient client, String prefix, ScheduledExecutorService scheduler) {
        this.client = Objects.requireNonNull(client, "client");
        if (prefix == null || prefix.isBlank()) {
            throw new IllegalArgumentException("prefix is blank");
        }
        this.prefix = prefix.endsWith("/") ? prefix : prefix + "/";
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    public void start(Consumer<KeyValueList> onChange) {
        Objects.requireNonNull(onChange, "onChange");
        if (!running.compareAndSet(false, true)) return;
        log.infof("Starting Consul watcher for '%s'", prefix);
        watchLoop(onChange, null, 0L);
    }

    private void watchLoop(Consumer<KeyValueList> onChange, Long lastIndex, long backoffMs) {
        if (!running.get()) return;

        BlockingQueryOptions opts = new BlockingQueryOptions()
                .setIndex(lastIndex)   // X-Consul-Index из предыдущего ответа
                .setWait("10m");       // максимум для blocking query

        client.getValuesWithOptions(prefix, opts, ar -> {
            if (!running.get()) return;

            if (ar.failed()) {
                long next = Math.min(backoffMs == 0 ? 500L : Math.min(backoffMs * 2, 5000L), 5000L);
                log.warnf("Consul watch error on '%s': %s (retry in %d ms)",
                        prefix, ar.cause().toString(), next);
                scheduler.schedule(() -> watchLoop(onChange, lastIndex, next), next, TimeUnit.MILLISECONDS);
                return;
            }

            KeyValueList kvs = ar.result();
            Long newIndex = kvs.getIndex();
            if (newIndex != null && (lastIndex == null || newIndex > lastIndex)) {
                try {
                    onChange.accept(kvs); // kvs.getList(): List<KeyValue>
                } catch (Throwable handlerEx) {
                    log.error("Error in onChange handler", handlerEx);
                }
            }

            // немедленно продолжаем цикл — следующий blocking-запрос
            scheduler.execute(() -> watchLoop(onChange, newIndex, 0L));
        });
    }


    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;
        log.infof("Stopping Consul watcher for '%s'", prefix);
        try {
            client.close();
        } catch (Exception ignore) {
        }
    }
}

