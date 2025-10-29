package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.DeclarativeKubernetesClient;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import com.netcracker.core.declarative.service.composite.consul.ConsulSnapshotHandler;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
@Slf4j
public class CompositeStructureToSecretHandler implements ConsulSnapshotHandler {
    private static final String SECRET_NAME = "current-composite-structure";

    private final DeclarativeKubernetesClient kubernetesClient;
    private final String namespace;

    private final ExecutorService k8sWritesExecutorService;

    @Inject
    public CompositeStructureToSecretHandler(KubernetesClient client,
                                             @ConfigProperty(name = "cloud.microservice.namespace") String namespace) {
        this.kubernetesClient = new DeclarativeKubernetesClient(client);
        this.namespace = namespace;
        this.k8sWritesExecutorService = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "core-operator-k8s-writes");
            t.setDaemon(true);
            return t;
        });
    }

    @PreDestroy
    void shutdownExecutor() {
        k8sWritesExecutorService.shutdown();
    }

    @Override
    public void handle(ConsulPrefixSnapshot snapshot) {
        log.info("VLLA SecretStateHandler handle, data = {}", snapshot);

        final String json = snapshot.toJson();

        k8sWritesExecutorService.submit(() -> {
            try {
                kubernetesClient.createOrUpdateSecret(SECRET_NAME, namespace, json, Collections.emptyMap());
            } catch (Exception e) {
                log.error("VLLA SecretStateHandler error", e);
                //todo vlla handle and retry
            }
        });
    }
}
