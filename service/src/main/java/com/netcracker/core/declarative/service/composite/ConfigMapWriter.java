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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
@Slf4j
public class ConfigMapWriter {
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final Duration INITIAL_RETRY_DELAY = Duration.ofSeconds(3);
    private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(30);

    private final ConfigMapClient configMapClient;
    private final String namespace;
    private final ScheduledExecutorService executor;
    private final AtomicReference<Map<String, String>> latestPayload;
    private final Object lock = new Object();
    private ScheduledFuture<?> scheduledTask;

    @Inject
    public ConfigMapWriter(ConfigMapClient configMapClient,
                           @ConfigProperty(name = "cloud.microservice.namespace") String namespace) {
        this.configMapClient = configMapClient;
        this.namespace = namespace;
        this.latestPayload = new AtomicReference<>();
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "configmap-writer");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void requestUpdate(String configMapName, Map<String, String> payload) {
        Objects.requireNonNull(payload, "payload");
        latestPayload.set(payload);
        scheduleLatest(configMapName, Duration.ZERO);
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }

    private void scheduleLatest(String configMapName, Duration delay) {
        synchronized (lock) {
            ScheduledFuture<?> current = scheduledTask;
            if (current != null) {
                current.cancel(true);
            }
            try {
                scheduledTask = executor.schedule(
                        () -> updateConfigMap(configMapName, latestPayload.get(), 1, INITIAL_RETRY_DELAY),
                        delay.toMillis(),
                        TimeUnit.MILLISECONDS
                );
            } catch (RejectedExecutionException ex) {
                log.debug("Config map updater executor is shut down, skipping update scheduling for '{}'", configMapName);
            }
        }
    }

    private void updateConfigMap(String configMapName, Map<String, String> payload, int attempt, Duration nextDelay) {
        if (payload == null) {
            return;
        }
        if (!Objects.equals(latestPayload.get(), payload)) {
            return;
        }
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

            Duration finalFollowingDelay = followingDelay;
            synchronized (lock) {
                if (!Objects.equals(latestPayload.get(), payload)) {
                    return;
                }
                try {
                    scheduledTask = executor.schedule(
                            () -> updateConfigMap(configMapName, payload, attempt + 1, finalFollowingDelay),
                            boundedDelay.toMillis(),
                            TimeUnit.MILLISECONDS
                    );
                } catch (RejectedExecutionException ree) {
                    log.debug("Failed to schedule retry for config map '{}' because executor is shut down", configMapName);
                }
            }
        }
    }
}
