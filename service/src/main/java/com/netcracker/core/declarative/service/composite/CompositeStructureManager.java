package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.ConfigMapClient;
import com.netcracker.core.declarative.service.composite.consul.ConsulClient;
import com.netcracker.core.declarative.service.composite.consul.ConsulSnapshotHandler;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
@Startup
@Slf4j
public class CompositeStructureManager {
    public static final String CONFIG_MAP_NAME = "composite-structure";

    private final CompositeWatcher compositeWatcher;
    private final ConfigMapClient configMapClient;
    private final String namespace;
    private boolean watcherRunning;

    @Inject
    public CompositeStructureManager(@ConfigProperty(name = "cloud.microservice.namespace") String namespace,
                                     ConsulClient consulClient,
                                     CompositeStructureToConfigMapHandler compositeStructureHandler,
                                     ConfigMapClient configMapClient) {
        this.namespace = namespace;
        this.configMapClient = configMapClient;
        ConsulSnapshotHandler guardedHandler = snapshot -> {
            if (shouldStopManagingConfigMap()) {
                log.info("Stopping composite structure polling because '{}' is no longer managed by core-operator.", CONFIG_MAP_NAME);
                stopWatcher();
                return;
            }
            compositeStructureHandler.handle(snapshot);
        };
        this.compositeWatcher = new CompositeWatcher(namespace, consulClient, guardedHandler);
    }

    @PostConstruct
    void start() {
        if (shouldStopManagingConfigMap()) {
            log.info("Composite structure polling is disabled because '{}' is no longer managed by core-operator.", CONFIG_MAP_NAME);
            return;
        }
        startWatcher();
    }

    @PreDestroy
    void stop() {
        stopWatcher();
    }

    private boolean shouldStopManagingConfigMap() {
        return !configMapClient.isManagedByCoreOperator(CONFIG_MAP_NAME, namespace);
    }

    private void startWatcher() {
        if (watcherRunning) {
            return;
        }
        compositeWatcher.start();
        watcherRunning = true;
    }

    private void stopWatcher() {
        if (!watcherRunning) {
            return;
        }
        compositeWatcher.stop();
        watcherRunning = false;
    }
}
