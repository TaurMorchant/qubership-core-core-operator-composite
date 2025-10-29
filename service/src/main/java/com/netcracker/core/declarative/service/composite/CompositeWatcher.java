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
    private static final KvPollConfig KV_POLL_CONFIG = KvPollConfig.builder()
            .wait(Duration.ofMinutes(9))
            .backoffMin(Duration.ofSeconds(1))
            .backoffMax(Duration.ofSeconds(30))
            .initialDelay(Duration.ZERO)
            .fireOnFirstSuccess(true)
            .build();

    private final String namespace;
    private final ConsulClient consulClient;
    private final StructurePrefixResolver prefixResolver = new StructurePrefixResolver();
    private final CompositeStructureStateHandler compositeStructureStateHandler;

    private ScheduledExecutorService scheduler;
    private KvLongPoller compositeStructureRefPoller;
    private KvLongPoller compositeStructurePoller;

    private volatile String currentPrefix;

    public CompositeWatcher(@ConfigProperty(name = "cloud.microservice.namespace") String namespace,
                            ConsulClientFactory consulClientFactory,
                            Instance<TokenStorage> tokenStorage,
                            SecretStateHandlerComposite compositeStructureStateHandler) {
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.consulClient = consulClientFactory.create(tokenStorage.get().get(), Duration.ofMinutes(10).toMillis());
        this.compositeStructureStateHandler = compositeStructureStateHandler;
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

        startCompositeStructureRefLongPool();
    }

    void startCompositeStructureRefLongPool() {
        final KvWatcher kvWatcher = new ConsulKvWatcher(consulClient);

        final String compositeStructureRefKey = COMPOSITE_STRUCTURE_REF_TEMPLATE.formatted(namespace);
        log.info("CompositeWatcher start: structureRef='{}'", compositeStructureRefKey);

        this.compositeStructureRefPoller = KvLongPoller.builder()
                .path(compositeStructureRefKey)
                .watcher(kvWatcher)
                .scheduler(scheduler)
                .pollConfig(KV_POLL_CONFIG)
                .onSnapshot(list -> onCompositeStructureRefSnapshot(compositeStructureRefKey, list))
                .build();
        compositeStructureRefPoller.start();
    }

    @PreDestroy
    void stop() {
        try {
            if (compositeStructureRefPoller != null) {
                compositeStructureRefPoller.stop();
            }
            if (compositeStructurePoller != null) {
                compositeStructurePoller.stop();
            }
        } finally {
            if (scheduler != null) {
                scheduler.shutdownNow();
                try {
                    scheduler.awaitTermination(5, TimeUnit.SECONDS);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        log.info("CompositeWatcher stopped.");
    }

    private void onCompositeStructureRefSnapshot(String refKey, KeyValueList list) {
        final String compositeStructurePrefix = prefixResolver.resolve(list, refKey);
        log.info("Current composite structure ref = '{}'", compositeStructurePrefix);
        startWatchCompositeStructure(compositeStructurePrefix);
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

        final KvWatcher watcher = new ConsulKvWatcher(consulClient);

        this.compositeStructurePoller = KvLongPoller.builder()
                .path(newPrefix)
                .watcher(watcher)
                .scheduler(scheduler)
                .pollConfig(KV_POLL_CONFIG)
                .onSnapshot(list -> {
                    log.info("VLLA compositeStructurePoller onSnapshot");
                    CompositeStructureState state = CompositeStructureState.from(list);
                    compositeStructureStateHandler.handle(state);
                })
                .build();
        compositeStructurePoller.start();
    }
}
