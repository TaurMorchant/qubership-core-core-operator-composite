package com.netcracker.core.declarative.service;

import com.netcracker.cloud.consul.provider.common.TokenStorage;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;

import io.vertx.ext.consul.BlockingQueryOptions;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
@Startup
@Slf4j
public class CompositeWatcher {

    private static final String STRUCTURE_REF_TEMPLATE = "config/%s/application/composite/structureRef";
    private static final String WAIT = "9s";              // Consul long-poll cap
    private static final long MAX_BACKOFF_MS = 30_000L;   // 30s

    private final String namespace;

    ConsulClient consul;

    // Сервис планирования (один daemon-поток)
    private ScheduledExecutorService scheduler;

    private volatile boolean running = false;

    // Индексы long-poll
    private final AtomicLong structureRefIndex = new AtomicLong(0);
    private final AtomicLong structureIndex = new AtomicLong(0);

    // Ссылки на запланированные задачи (для отмены при стопе)
    private ScheduledFuture<?> structureRefFuture;
    private ScheduledFuture<?> structureFuture;

    // Текущее состояние
    private volatile String currentStructurePrefix = null;
    private Map<String, Long> lastSnapshot = new HashMap<>();
    private volatile boolean initialStructureLoad = false;

    public CompositeWatcher(@ConfigProperty(name = "cloud.microservice.namespace") String namespace,
                            ConsulClientFactory consulClientFactory,
                            Instance<TokenStorage> consulTokenStorage) {
        this.namespace = namespace;
        this.consul = consulClientFactory.create(consulTokenStorage.get().get(), 10 * 1000);
    }

    @PostConstruct
    void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "composite-watcher");
            t.setDaemon(true);
            return t;
        });

        running = true;
        String structureRefKey = STRUCTURE_REF_TEMPLATE.formatted(namespace);
        log.info("VLLA CompositeWatcher start. structureRef key: {}", structureRefKey);
        scheduleStructureRefWatch(structureRefKey, 0);
    }

    @PreDestroy
    void stop() {
        running = false;
        if (structureRefFuture != null) structureRefFuture.cancel(true);
        if (structureFuture != null) structureFuture.cancel(true);
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {}
        }
        log.info("VLLA CompositeWatcher stopped.");
    }

    // ---------- WATCH structureRef ----------

    private void scheduleStructureRefWatch(String key, long delayMs) {
        log.debug("VLLA scheduleStructureRefWatch {}", key);
        if (!running) return;
        if (structureRefFuture != null) structureRefFuture.cancel(false);
        structureRefFuture = scheduler.schedule(() -> watchStructureRefOnce(key, 0), Math.max(0, delayMs), TimeUnit.MILLISECONDS);
    }

    private void watchStructureRefOnce(String key, long backoffMs) {
        log.debug("VLLA watchStructureRefOnce {}", key);
        if (!running) return;

        BlockingQueryOptions bq = new BlockingQueryOptions()
                .setIndex(structureRefIndex.get())
                .setWait(WAIT);

        // Читаем как список (на полный ключ), чтобы иметь доступ к консуловскому X-Consul-Index через KeyValueList.getIndex()
        consul.getValuesWithOptions(key, bq, ar -> {
            if (!running) return;

            log.debug("VLLA getValuesWithOptions {}", key);

            if (ar.succeeded()) {
                KeyValueList list = ar.result();
                long newIndex = (list != null) ? list.getIndex() : 0L;
                long oldIndex = structureRefIndex.get();

                // Следующий long-poll планируем сразу (предыдущий уже "ждал")
                scheduleStructureRefWatch(key, 0);

                if (newIndex > oldIndex) {
                    structureRefIndex.set(newIndex);

                    String newPrefix = null;
                    if (list != null && list.getList() != null) {
                        for (KeyValue kv : list.getList()) {
                            if (key.equals(kv.getKey())) {
                                String val = kv.getValue();
                                if (val != null && !val.isBlank()) newPrefix = val.trim();
                                break;
                            }
                        }
                    }
                    log.info("structureRef changed (index {} -> {}). New prefix: {}", oldIndex, newIndex, newPrefix);
                    switchStructurePrefix(newPrefix);
                }
            } else {
                long next = nextBackoff(backoffMs);
                log.warn("Consul watch error on '{}' (structureRef). Retry in {} ms. Details: {}", key, next, ar.cause());
                scheduleStructureRefWatch(key, next);
            }
        });
    }

    private void switchStructurePrefix(String newPrefix) {
        log.debug("VLLA switchStructurePrefix {}", newPrefix);

        if (structureFuture != null) {
            structureFuture.cancel(false);
            structureFuture = null;
        }
        structureIndex.set(0);
        lastSnapshot = new HashMap<>();
        initialStructureLoad = true;

        currentStructurePrefix = newPrefix;

        if (newPrefix == null || newPrefix.isBlank()) {
            log.warn("structureRef is empty/null — structure watch paused until it appears.");
            return;
        }

        log.info("Switching structure watch to prefix: {}", newPrefix);
        scheduleStructureWatch(newPrefix, 0);
    }

    // ---------- WATCH structure prefix ----------

    private void scheduleStructureWatch(String prefix, long delayMs) {
        log.debug("VLLA scheduleStructureWatch {}", prefix);
        if (!running) return;
        if (structureFuture != null) structureFuture.cancel(false);
        structureFuture = scheduler.schedule(() -> watchStructureOnce(prefix, 0), Math.max(0, delayMs), TimeUnit.MILLISECONDS);
    }

    private void watchStructureOnce(String prefix, long backoffMs) {
        log.debug("VLLA watchStructureOnce {}", prefix);
        if (!running) return;

        final long prevIndex = structureIndex.get();

        BlockingQueryOptions bq = new BlockingQueryOptions()
                .setIndex(prevIndex)
                .setWait(WAIT);

        consul.getValuesWithOptions(prefix, bq, ar -> {
            if (!running) return;

            log.debug("VLLA getValuesWithOptions {}", prefix);

            if (ar.succeeded()) {
                KeyValueList list = ar.result();
                long newIndex = (list != null) ? list.getIndex() : 0L;

                // Планируем следующий long-poll
                scheduleStructureWatch(prefix, 0);

                List<KeyValue> entries = (list != null && list.getList() != null) ? list.getList() : List.of();

                Map<String, Long> newSnapshot = new HashMap<>(entries.size());
                for (KeyValue e : entries) {
                    long mi = e.getModifyIndex();
                    newSnapshot.put(e.getKey(), mi);
                }

                // 1) Первичная инициализация — всегда один раз для нового префикса
                if (initialStructureLoad) {
                    log.info("[COMPOSITE] INITIALIZE snapshot for '{}' (keys={})", prefix, newSnapshot.size());
                    // initializeStore(newSnapshot, entries); // <- ваша инициализация
                    logStructureDiff(Collections.emptyMap(), newSnapshot);

                    lastSnapshot = newSnapshot;
                    structureIndex.set(newIndex);
                    initialStructureLoad = false;
                    return;
                }

                // 2) Обычная реакция только на изменения индекса
                if (newIndex > prevIndex) {
                    structureIndex.set(newIndex);
                    logStructureDiff(lastSnapshot, newSnapshot);
                    lastSnapshot = newSnapshot;
                }
            } else {
                long next = nextBackoff(backoffMs);
                log.warn("Consul watch error on prefix '{}'. Retry in {} ms. Details: {}", prefix, next, ar.cause());
                scheduleStructureWatch(prefix, next);
            }
        });
    }

    // ---------- Утилиты ----------

    private void logStructureDiff(Map<String, Long> oldSnap, Map<String, Long> newSnap) {
        for (String k : newSnap.keySet()) {
            if (!oldSnap.containsKey(k)) {
                log.info("[COMPOSITE] ADDED: {}", k);
            }
        }
        for (String k : oldSnap.keySet()) {
            if (!newSnap.containsKey(k)) {
                log.info("[COMPOSITE] REMOVED: {}", k);
            }
        }
        for (Map.Entry<String, Long> e : newSnap.entrySet()) {
            Long oldIdx = oldSnap.get(e.getKey());
            if (oldIdx != null && !Objects.equals(oldIdx, e.getValue())) {
                log.info("[COMPOSITE] MODIFIED: {} (modifyIndex {} -> {})", e.getKey(), oldIdx, e.getValue());
            }
        }
    }

    private long nextBackoff(long current) {
        if (current <= 0) return 1_000L;
        return Math.min(current * 2, MAX_BACKOFF_MS);
    }
}
