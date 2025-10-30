package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.DeclarativeKubernetesClient;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulSnapshotSerializationException;
import com.netcracker.core.declarative.service.composite.consul.ConsulSnapshotHandler;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;

@ApplicationScoped
@Slf4j
public class CompositeStructureToSecretHandler implements ConsulSnapshotHandler {
    private static final String SECRET_NAME = "current-composite-structure";
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final Duration INITIAL_RETRY_DELAY = Duration.ofSeconds(3);
    private static final Duration MAX_RETRY_DELAY = Duration.ofSeconds(30);

    private final DeclarativeKubernetesClient kubernetesClient;
    private final String namespace;

    private final ScheduledThreadPoolExecutor k8sWritesExecutorService;

    @Inject
    public CompositeStructureToSecretHandler(KubernetesClient client,
                                             @ConfigProperty(name = "cloud.microservice.namespace") String namespace) {
        this.kubernetesClient = new DeclarativeKubernetesClient(client);
        this.namespace = namespace;
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
    }

    @Override
    public void handle(ConsulPrefixSnapshot compositeStructureSnapshot) {
        log.info("Store Composite Structure to secret {}", SECRET_NAME);
        try {
            k8sWritesExecutorService.getQueue().clear();
            k8sWritesExecutorService.execute(() -> {
                try {
                    String json = compositeStructureSnapshot.toJson();
                    Map<String, String> compositeStructureContent = Map.of("compositeStructure", json);
                    updateSecretWithRetry(compositeStructureContent, 1, INITIAL_RETRY_DELAY);
                } catch (ConsulSnapshotSerializationException e) {
                    log.error("Failed to serialize Consul snapshot for secret '{}'", SECRET_NAME, e);
                }
            });
        } catch (RejectedExecutionException ex) {
            log.warn("K8s writes executor is shut down, skipping secret update for '{}'", SECRET_NAME, ex);
        }
    }

    private void updateSecretWithRetry(Map<String, String> compositeStructure, int attempt, Duration nextDelay) {
        try {
            kubernetesClient.createOrUpdateSecret(SECRET_NAME, namespace, compositeStructure, null);
        } catch (Exception e) {
            if (attempt >= MAX_RETRY_ATTEMPTS) {
                log.error("Failed to update secret '{}' after {} attempts", SECRET_NAME, attempt, e);
                return;
            }

            Duration boundedDelay = nextDelay.compareTo(MAX_RETRY_DELAY) > 0 ? MAX_RETRY_DELAY : nextDelay;
            log.warn("Failed to update secret '{}' on attempt {}/{}. Retrying in {}.",
                    SECRET_NAME, attempt, MAX_RETRY_ATTEMPTS, boundedDelay, e);

            Duration followingDelay = nextDelay.multipliedBy(2);
            if (followingDelay.compareTo(MAX_RETRY_DELAY) > 0) {
                followingDelay = MAX_RETRY_DELAY;
            }

            Duration finalFollowingDelay = followingDelay;
            try {
                k8sWritesExecutorService.schedule(
                        () -> updateSecretWithRetry(compositeStructure, attempt + 1, finalFollowingDelay),
                        boundedDelay.toMillis(),
                        TimeUnit.MILLISECONDS
                );
            } catch (RejectedExecutionException ree) {
                log.warn("Failed to schedule retry for secret '{}' because executor is shut down", SECRET_NAME, ree);
            }
        }
    }
}
