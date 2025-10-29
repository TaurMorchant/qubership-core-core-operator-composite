package com.netcracker.core.declarative.service.composite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netcracker.core.declarative.client.k8s.DeclarativeKubernetesClient;
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
public class SecretStateHandlerComposite implements CompositeStructureStateHandler {
    private final DeclarativeKubernetesClient k8s;
    private final String namespace;

    private static final String SECRET_NAME = "current-composite-structure";

    private final ExecutorService k8sWrites;

    @Inject
    public SecretStateHandlerComposite(KubernetesClient client,
                                       @ConfigProperty(name = "cloud.microservice.namespace") String namespace) {
        this.k8s = new DeclarativeKubernetesClient(client);
        this.namespace = namespace;
        this.k8sWrites = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "core-operator-k8s-writes");
            t.setDaemon(true);
            return t;
        });
    }

    @PreDestroy
    void shutdownExecutor() {
        k8sWrites.shutdown();
    }

    @Override
    public void handle(CompositeStructureState state) {
        log.info("VLLA SecretStateHandler handle, data = {}", state.data());

        try {
            final String json = state.toJson();

            k8sWrites.submit(() -> {
                try {
                    k8s.createOrUpdateSecret(SECRET_NAME, namespace, json, Collections.emptyMap());
                } catch (Exception e) {
                    log.error("VLLA SecretStateHandler error", e);
                    //todo vlla handle and retry
                }
            });
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);//todo vlla
        }
    }
}
