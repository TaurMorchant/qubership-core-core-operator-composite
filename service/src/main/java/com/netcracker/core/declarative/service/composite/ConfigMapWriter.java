package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.ConfigMapClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
@Slf4j
public class ConfigMapWriter {
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final Duration INITIAL_RETRY_DELAY = Duration.ofSeconds(3);
    private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(30);

    private final ConfigMapClient configMapClient;
    private final String namespace;
    private final ScheduledExecutorService executor;

    @Inject
    public ConfigMapWriter(ConfigMapClient configMapClient,
                           @ConfigProperty(name = "cloud.microservice.namespace") String namespace) {
        this.configMapClient = configMapClient;
        this.namespace = namespace;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "configmap-writer");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void requestUpdate(String configMapName, Map<String, String> payload) {
        Objects.requireNonNull(configMapName, "configMapName");
        Objects.requireNonNull(payload, "payload");
        Map<String, String> snapshot = Map.copyOf(payload);
        scheduleUpdate(configMapName, snapshot, 1, Duration.ZERO, INITIAL_RETRY_DELAY);
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }

    private void scheduleUpdate(String configMapName,
                                Map<String, String> payload,
                                int attempt,
                                Duration delay,
                                Duration nextDelay) {
        try {
            executor.schedule(
                    () -> updateConfigMap(configMapName, payload, attempt, nextDelay),
                    delay.toMillis(),
                    TimeUnit.MILLISECONDS
            );
        } catch (RejectedExecutionException ex) {
            log.debug("Config map updater executor is shut down, skipping update scheduling for '{}'", configMapName);
        }
    }

    private void updateConfigMap(String configMapName, Map<String, String> payload, int attempt, Duration nextDelay) {
        try {
            configMapClient.createOrUpdate(configMapName, namespace, payload, null);
        } catch (KubernetesClientException ex) {
            if (attempt >= MAX_RETRY_ATTEMPTS) {
                log.error("Failed to update config map '{}' after {} attempts", configMapName, attempt, ex);
                return;
            }

            Duration boundedDelay = nextDelay.compareTo(MAX_RETRY_DELAY) > 0 ? MAX_RETRY_DELAY : nextDelay;
            log.warn("Failed to update config map '{}' on attempt {}/{}. Retrying in {}.",
                    configMapName, attempt, MAX_RETRY_ATTEMPTS, boundedDelay, ex);

            Duration followingDelay = nextDelay.multipliedBy(2);
            if (followingDelay.compareTo(MAX_RETRY_DELAY) > 0) {
                followingDelay = MAX_RETRY_DELAY;
            }

            scheduleUpdate(configMapName, payload, attempt + 1, boundedDelay, followingDelay);
        }
    }
}
