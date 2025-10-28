package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.DeclarativeKubernetesClient;
import com.netcracker.core.declarative.service.kv.KvLongPoller;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
@Slf4j
public class ConfigMapStateHandler implements StructureStateHandler {
    private final DeclarativeKubernetesClient k8s;
    private final String namespace;
    private final AtomicReference<String> lastJson = new AtomicReference<>();

    private static final String CM_NAME = "VLLA-TEST-CONFIG-MAP";

    ExecutorService k8sWrites = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "core-operator-k8s-writes");
        t.setDaemon(true);
        return t;
    });

    @Inject
    public ConfigMapStateHandler(KubernetesClient client,
                                 @ConfigProperty(name = "cloud.microservice.namespace") String namespace) {
        this.k8s = new DeclarativeKubernetesClient(client);
        this.namespace = namespace;
    }

    @Override
    public void handle(StructureState state, KvLongPoller.IndexPair idx, boolean initial) {
        log.info("VLLA ConfigMapStateHandler handle, data = {}", state.data());
        // 1) сериализация состояния
        String json = state.data().toString();

        // 2) простейший коалесинг (не пишем, если значение не изменилось)
        String prev = lastJson.getAndSet(json);
        if (prev != null && prev.equals(json) && !initial) {
            return; // ничего не менять в конфиг-мапе
        }

        k8sWrites.submit(() -> {
            try {
                k8s.createOrUpdateConfigMap(CM_NAME, namespace, json, Collections.emptyMap());
            }
            catch (Exception e) {
                log.error("VLLA ConfigMapStateHandler error", e);
            }
        });
    }
}
