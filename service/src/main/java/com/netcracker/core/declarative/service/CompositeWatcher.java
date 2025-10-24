package com.netcracker.core.declarative.service;

import io.vertx.ext.consul.BlockingQueryOptions;
import io.vertx.ext.consul.ConsulClient;
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
    private final String prefix;
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
        log.info("Starting Consul watcher for '{}'", prefix);
        watchLoop(onChange, null, 0L);
    }

    private void watchLoop(Consumer<KeyValueList> onChange, Long lastIndex, long backoffMs) {
        if (!running.get()) return;

        BlockingQueryOptions opts = new BlockingQueryOptions().setWait("10m");

        long effectiveIndex = (lastIndex == null ? 0L : lastIndex);
        opts.setIndex(effectiveIndex);

        client.getValuesWithOptions(prefix, opts, ar -> {
            if (!running.get()) return;

            if (ar.failed()) {
                long next = Math.min(backoffMs == 0 ? 500L : Math.min(backoffMs * 2, 5000L), 5000L);
                log.warn("Consul watch error on '{}': {} (retry in {} ms)", prefix, ar.cause().toString(), next);
                scheduler.schedule(() -> watchLoop(onChange, lastIndex, next), next, TimeUnit.MILLISECONDS);
                return;
            }

            KeyValueList kvs = ar.result();
            Long returnedIndex = kvs.getIndex();

            log.debug("VLLA get KeyValueList = {}", kvs);

            if (lastIndex == null || returnedIndex > lastIndex) {
                try {
                    onChange.accept(kvs);
                } catch (Throwable handlerEx) {
                    log.error("Error in onChange handler", handlerEx);
                }
            }

            scheduler.execute(() -> watchLoop(onChange, returnedIndex, 0L));
        });
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;
        log.info("Stopping Consul watcher for '{}'", prefix);
        scheduler.shutdownNow();
        try {
            client.close();
        } catch (Exception ignore) {
        }
    }
}
