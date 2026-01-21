package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.ConfigMapClient;
import com.netcracker.core.declarative.service.composite.consul.ConsulClient;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coordinates composite structure watching based on ConfigMap ownership.
 * <p>
 * Periodically checks if the {@value #CONFIG_MAP_NAME} ConfigMap is managed by core-operator.
 * If managed, starts the {@link CompositeStructureWatcher}; otherwise stops it.
 * This allows another operator to take over ConfigMap management when needed.
 */
@ApplicationScoped
@Startup
@Slf4j
public class CompositeStructureWatchCoordinator {
    public static final String CONFIG_MAP_NAME = "composite-structure";
    private static final Duration CONFIG_MAP_MANAGEMENT_CHECK_INTERVAL = Duration.ofMinutes(5);

    private final CompositeStructureWatcher compositeStructureWatcher;
    private final ConfigMapClient configMapClient;
    private final String namespace;
    private final AtomicBoolean watcherRunning;
    private final ScheduledExecutorService configMapManagementCheckExecutor;

    @Inject
    public CompositeStructureWatchCoordinator(@ConfigProperty(name = "cloud.microservice.namespace") String namespace,
                                              ConsulClient consulClient,
                                              CompositeStructureSnapshotHandler compositeStructureHandler,
                                              ConfigMapClient configMapClient) {
        this.namespace = namespace;
        this.configMapClient = configMapClient;
        this.compositeStructureWatcher = new CompositeStructureWatcher(namespace, consulClient, compositeStructureHandler);
        this.watcherRunning = new AtomicBoolean(false);
        this.configMapManagementCheckExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "composite-structure-config-map-management-check");
            thread.setDaemon(true);
            return thread;
        });
    }

    @PostConstruct
    void start() {
        configMapManagementCheckExecutor.scheduleWithFixedDelay(
                this::ensureWatcherState,
                0,
                CONFIG_MAP_MANAGEMENT_CHECK_INTERVAL.toMinutes(),
                TimeUnit.MINUTES
        );
    }

    @PreDestroy
    void stop() {
        configMapManagementCheckExecutor.shutdownNow();
        stopWatcher();
    }

    private void ensureWatcherState() {
        try {
            boolean shouldManage = configMapClient.isManagedByCoreOperator(CONFIG_MAP_NAME, namespace);
            if (shouldManage) {
                startWatcher();
            } else {
                log.debug("Composite structure polling is disabled because '{}' is not managed by core-operator.", CONFIG_MAP_NAME);
                stopWatcher();
            }
        } catch (RuntimeException ex) {
            log.warn("Failed to verify management state for '{}'. Retrying on next schedule.", CONFIG_MAP_NAME, ex);
        }
    }

    private void startWatcher() {
        if (!watcherRunning.compareAndSet(false, true)) {
            return;
        }
        compositeStructureWatcher.start();
    }

    private void stopWatcher() {
        if (!watcherRunning.compareAndSet(true, false)) {
            return;
        }
        compositeStructureWatcher.stop();
    }
}
