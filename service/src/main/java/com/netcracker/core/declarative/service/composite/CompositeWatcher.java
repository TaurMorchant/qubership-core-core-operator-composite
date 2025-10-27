package com.netcracker.core.declarative.service.composite;


import com.netcracker.cloud.consul.provider.common.TokenStorage;
import com.netcracker.core.declarative.service.ConsulClientFactory;
import com.netcracker.core.declarative.service.kv.*;
import io.quarkus.runtime.Startup;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueList;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;

@ApplicationScoped
@Startup
@Slf4j
public class CompositeWatcher {

    private static final String STRUCTURE_REF_TEMPLATE = "config/%s/application/composite/structureRef";

    private final String namespace;
    private final ConsulClient consul;
    private final StructurePrefixResolver prefixResolver = new StructurePrefixResolver();
    private final StructureStateBuilder stateBuilder = new StructureStateBuilder();
    private final StructureStateHandler stateHandler; // инжектируйте свою реализацию

    // инфраструктура
    private ScheduledExecutorService scheduler;
    private KvLongPoller structureRefPoller;
    private KvLongPoller structurePoller;

    // текущее наблюдаемое состояние
    private volatile String currentPrefix;

    public CompositeWatcher(@ConfigProperty(name = "cloud.microservice.namespace") String namespace,
                            ConsulClientFactory consulClientFactory,
                            Instance<TokenStorage> tokenStorage) {
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.consul = Objects.requireNonNull(
                consulClientFactory.create(tokenStorage.get().get(), 10_000), "consul");
        this.stateHandler = new LogStateHandler();
    }

    @PostConstruct
    void start() {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "composite-watcher");
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((th, ex) ->
                    log.error("Uncaught exception in '{}'", th.getName(), ex));
            return t;
        });

        final KvClient kv = new VertxKvClient(consul);
        final KvPollConfig cfg = KvPollConfig.builder()
                .wait(Duration.ofSeconds(9))
                .backoffMin(Duration.ofSeconds(1))
                .backoffMax(Duration.ofSeconds(30))
                .initialDelay(Duration.ZERO)
                .fireOnFirstSuccess(true)
                .build();

        final String structureRefKey = STRUCTURE_REF_TEMPLATE.formatted(namespace);
        log.info("CompositeWatcher start: structureRef='{}'", structureRefKey);

        // 1) Поллер за ключом structureRef
        this.structureRefPoller = KvLongPoller.builder()
                .path(structureRefKey)
                .client(kv)
                .scheduler(scheduler)
                .cfg(cfg)
                .onState(s -> log.debug("structureRef poller state={}", s))
                .onSnapshot((list, idx) -> onStructureRefSnapshot(structureRefKey, list, idx))
                .build();
        structureRefPoller.start();
    }

    @PreDestroy
    void stop() {
        try {
            if (structureRefPoller != null) structureRefPoller.stop();
            if (structurePoller != null) structurePoller.stop();
        } finally {
            if (scheduler != null) {
                scheduler.shutdownNow();
                try { scheduler.awaitTermination(5, TimeUnit.SECONDS); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
        log.info("CompositeWatcher stopped.");
    }

    private void onStructureRefSnapshot(String refKey, KeyValueList list, KvLongPoller.IndexPair idx) {
        final String newPrefix = prefixResolver.resolve(list, refKey).orElse(null);
        log.info("structureRef observed {} -> {}, prefix='{}'", idx.prev(), idx.now(), newPrefix);
        switchStructurePrefix(newPrefix);
    }

    private void switchStructurePrefix(String newPrefix) {
        if (Objects.equals(currentPrefix, newPrefix)) {
            log.debug("structureRef unchanged: '{}'", newPrefix);
            return;
        }
        if (structurePoller != null) {
            structurePoller.stop();
            structurePoller = null;
        }
        currentPrefix = newPrefix;

        if (newPrefix == null || newPrefix.isBlank()) {
            log.warn("structureRef empty — structure polling paused.");
            return;
        }

        log.info("Switching structure polling to prefix='{}'", newPrefix);

        final KvClient kv = new VertxKvClient(consul);
        final KvPollConfig cfg = KvPollConfig.builder()
                .wait(Duration.ofSeconds(9))
                .backoffMin(Duration.ofSeconds(1))
                .backoffMax(Duration.ofSeconds(30))
                .initialDelay(Duration.ZERO)
                .fireOnFirstSuccess(true)   // важно: выполнить инициализацию сразу
                .build();

        // 2) Поллер по префиксу структуры — никакого diff: каждый снимок → полное состояние
        this.structurePoller = KvLongPoller.builder()
                .path(newPrefix)
                .client(kv)
                .scheduler(scheduler)
                .cfg(cfg)
                .onState(s -> log.debug("structure poller state={}", s))
                .onSnapshot((list, idx) -> {
                    boolean initial = (idx.prev() == 0); // первый успешный снапшот после переключения
                    StructureState state = stateBuilder.build(newPrefix, list);
                    stateHandler.handle(state, idx, initial);
                })
                .build();
        structurePoller.start();
    }
}
