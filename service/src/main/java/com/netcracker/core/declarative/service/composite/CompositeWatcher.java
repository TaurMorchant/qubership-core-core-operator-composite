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

    private static final String COMPOSITE_STRUCTURE_REF_TEMPLATE = "config/%s/application/composite/structureRef";

    private final String namespace;
    private final ConsulClient consulClient;
    private final StructurePrefixResolver prefixResolver = new StructurePrefixResolver();
    private final StructureStateHandler stateHandler;

    // инфраструктура
    private ScheduledExecutorService scheduler;
    private KvLongPoller compositeStructureRefPoller;
    private KvLongPoller compositeStructurePoller;

    // текущее наблюдаемое состояние
    private volatile String currentPrefix;

    public CompositeWatcher(@ConfigProperty(name = "cloud.microservice.namespace") String namespace,
                            ConsulClientFactory consulClientFactory,
                            Instance<TokenStorage> tokenStorage,
                            SecretStateHandler stateHandler) {
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.consulClient = Objects.requireNonNull(
                consulClientFactory.create(tokenStorage.get().get(), 60 * 1000), "consul");
        this.stateHandler = stateHandler;
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

        final KvWatcher kv = new ConsulKvWatcher(consulClient);
        final KvPollConfig cfg = KvPollConfig.builder()
                .wait(Duration.ofSeconds(50))
                .backoffMin(Duration.ofSeconds(1))
                .backoffMax(Duration.ofSeconds(30))
                .initialDelay(Duration.ZERO)
                .fireOnFirstSuccess(true)
                .build();

        final String compositeStructureRefKey = COMPOSITE_STRUCTURE_REF_TEMPLATE.formatted(namespace);
        log.info("CompositeWatcher start: structureRef='{}'", compositeStructureRefKey);

        // 1) Поллер за ключом structureRef
        this.compositeStructureRefPoller = KvLongPoller.builder()
                .path(compositeStructureRefKey)
                .client(kv)
                .scheduler(scheduler)
                .cfg(cfg)
                .onState(s -> log.debug("structureRef poller state={}", s))
                .onSnapshot(list -> onCompositeStructureRefSnapshot(compositeStructureRefKey, list))
                .build();
        compositeStructureRefPoller.start();
    }

    @PreDestroy
    void stop() {
        try {
            if (compositeStructureRefPoller != null) compositeStructureRefPoller.stop();
            if (compositeStructurePoller != null) compositeStructurePoller.stop();
        } finally {
            if (scheduler != null) {
                scheduler.shutdownNow();
                try { scheduler.awaitTermination(5, TimeUnit.SECONDS); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
        log.info("CompositeWatcher stopped.");
    }

    private void onCompositeStructureRefSnapshot(String refKey, KeyValueList list) {
        final String newCompositeStructurePrefix = prefixResolver.resolve(list, refKey).orElse(null);
        log.info("Current composite structure ref = '{}'", newCompositeStructurePrefix);
        startWatchCompositeStructure(newCompositeStructurePrefix);
    }

    private void startWatchCompositeStructure(String newPrefix) {
        if (Objects.equals(currentPrefix, newPrefix)) {
            log.debug("structureRef unchanged: '{}'", newPrefix);
            return;
        }
        if (compositeStructurePoller != null) {
            compositeStructurePoller.stop();
            compositeStructurePoller = null;
        }
        currentPrefix = newPrefix;

        if (newPrefix == null || newPrefix.isBlank()) {
            log.warn("structureRef empty — structure polling paused.");
            return;
        }

        log.info("Switching structure polling to prefix='{}'", newPrefix);

        final KvWatcher kv = new ConsulKvWatcher(consulClient);
        final KvPollConfig cfg = KvPollConfig.builder()
                .wait(Duration.ofSeconds(50))
                .backoffMin(Duration.ofSeconds(1))
                .backoffMax(Duration.ofSeconds(30))
                .initialDelay(Duration.ZERO)
                .fireOnFirstSuccess(true)   // важно: выполнить инициализацию сразу
                .build();

        // 2) Поллер по префиксу структуры — никакого diff: каждый снимок → полное состояние
        this.compositeStructurePoller = KvLongPoller.builder()
                .path(newPrefix)
                .client(kv)
                .scheduler(scheduler)
                .cfg(cfg)
                .onState(s -> log.debug("structure poller state={}", s))
                .onSnapshot(list -> {
//                    boolean initial = (idx.prev() == 0); // первый успешный снапшот после переключения
                    CompositeStructureState state = CompositeStructureState.from(list);
                    stateHandler.handle(state);
                })
                .build();
        compositeStructurePoller.start();
    }
}
