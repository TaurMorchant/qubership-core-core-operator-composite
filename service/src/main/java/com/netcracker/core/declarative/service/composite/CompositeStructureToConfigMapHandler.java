package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.ConfigMapClient;
import com.netcracker.core.declarative.service.composite.consul.ConsulSnapshotHandler;
import com.netcracker.core.declarative.service.composite.consul.model.CompositeStructureConfigMapPayload;
import com.netcracker.core.declarative.service.composite.consul.model.CompositeStructurePayload;
import com.netcracker.core.declarative.service.composite.consul.model.CompositeStructureSerializer;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulSnapshotSerializationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
@Slf4j
public class CompositeStructureToConfigMapHandler implements ConsulSnapshotHandler {
    private static final String CONFIG_MAP_NAME = "composite-structure";
    private static final String CONFIG_MAP_DATA_KEY = "data";
    private static final String DEFAULT_CLOUD_PROVIDER = "OnPrem";
    private static final String DEFAULT_CLOUD_OIDC_PROXY_URL = "http://super-proxy.namespace:8080";
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final Duration INITIAL_RETRY_DELAY = Duration.ofSeconds(3);
    private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(30);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ConfigMapClient configMapClient;
    private final String namespace;
    private final String cloudProvider;
    private final String cloudOidcProxyUrl;

    private final ScheduledThreadPoolExecutor k8sWritesExecutorService;

    @Inject
    public CompositeStructureToConfigMapHandler(ConfigMapClient configMapClient,
                                                @ConfigProperty(name = "cloud.microservice.namespace") String namespace,
                                                @ConfigProperty(name = "CLOUD_PROVIDER", defaultValue = DEFAULT_CLOUD_PROVIDER) String cloudProvider,
                                                @ConfigProperty(name = "CLOUD_OIDC_PROXY_URL",
                                                        defaultValue = DEFAULT_CLOUD_OIDC_PROXY_URL) String cloudOidcProxyUrl) {
        this.configMapClient = configMapClient;
        this.namespace = namespace;
        this.cloudProvider = cloudProvider;
        this.cloudOidcProxyUrl = cloudOidcProxyUrl;
        this.k8sWritesExecutorService = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "core-operator-k8s-writes");
            t.setDaemon(true);
            return t;
        });
        this.k8sWritesExecutorService.setRemoveOnCancelPolicy(true);
        this.k8sWritesExecutorService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.k8sWritesExecutorService.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }

    @PreDestroy
    void shutdownExecutor() {
        k8sWritesExecutorService.shutdown();
        try {
            if (!k8sWritesExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
                k8sWritesExecutorService.shutdownNow();
                if (!k8sWritesExecutorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.debug("K8s writes executor did not terminate in time after shutdown");
                }
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            log.debug("Interrupted while waiting for k8s writes executor shutdown");
        }
    }

    @Override
    public void handle(ConsulPrefixSnapshot compositeStructureSnapshot) {
        log.info("Store Composite Structure to config map {}", CONFIG_MAP_NAME);
        try {
            k8sWritesExecutorService.getQueue().clear();
            k8sWritesExecutorService.execute(() -> {
                try {
                    CompositeStructurePayload compositePayload = CompositeStructureSerializer.toPayload(compositeStructureSnapshot);
                    CompositeStructureConfigMapPayload payload = new CompositeStructureConfigMapPayload(
                            cloudProvider,
                            cloudOidcProxyUrl,
                            compositePayload
                    );
                    String json = serializePayload(payload);
                    Map<String, String> compositeStructureContent = Map.of(CONFIG_MAP_DATA_KEY, json);
                    updateConfigMapWithRetry(compositeStructureContent, 1, INITIAL_RETRY_DELAY);
                } catch (ConsulSnapshotSerializationException | JsonProcessingException e) {
                    log.error("Failed to serialize Consul snapshot for config map '{}'", CONFIG_MAP_NAME, e);
                }
            });
        } catch (RejectedExecutionException ex) {
            log.debug("K8s writes executor is shut down, skipping config map update for '{}'", CONFIG_MAP_NAME);
        }
    }

    void updateConfigMapWithRetry(Map<String, String> compositeStructure, int attempt, Duration nextDelay) {
        try {
            configMapClient.createOrUpdate(CONFIG_MAP_NAME, namespace, compositeStructure, null);
        } catch (Exception e) {
            if (attempt >= MAX_RETRY_ATTEMPTS) {
                log.error("Failed to update config map '{}' after {} attempts", CONFIG_MAP_NAME, attempt, e);
                return;
            }

            Duration boundedDelay = nextDelay.compareTo(MAX_RETRY_DELAY) > 0 ? MAX_RETRY_DELAY : nextDelay;
            log.warn("Failed to update config map '{}' on attempt {}/{}. Retrying in {}.",
                    CONFIG_MAP_NAME, attempt, MAX_RETRY_ATTEMPTS, boundedDelay, e);

            Duration followingDelay = nextDelay.multipliedBy(2);
            if (followingDelay.compareTo(MAX_RETRY_DELAY) > 0) {
                followingDelay = MAX_RETRY_DELAY;
            }

            Duration finalFollowingDelay = followingDelay;
            try {
                k8sWritesExecutorService.schedule(
                        () -> updateConfigMapWithRetry(compositeStructure, attempt + 1, finalFollowingDelay),
                        boundedDelay.toMillis(),
                        TimeUnit.MILLISECONDS
                );
            } catch (RejectedExecutionException ree) {
                log.debug("Failed to schedule retry for config map '{}' because executor is shut down", CONFIG_MAP_NAME);
            }
        }
    }

    private static String serializePayload(CompositeStructureConfigMapPayload payload) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(payload);
    }
}
