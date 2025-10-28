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
public class SecretStateHandler implements StructureStateHandler {
    private final DeclarativeKubernetesClient k8s;
    private final String namespace;
    private final AtomicReference<String> lastJson = new AtomicReference<>();

    private static final String SECRET_NAME = "vlla-test-secret";

    ExecutorService k8sWrites = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "core-operator-k8s-writes");
        t.setDaemon(true);
        return t;
    });

    @Inject
    public SecretStateHandler(KubernetesClient client,
                              @ConfigProperty(name = "cloud.microservice.namespace") String namespace) {
        this.k8s = new DeclarativeKubernetesClient(client);
        this.namespace = namespace;
    }

    @Override
    public void handle(CompositeStructureState state) {
        log.info("VLLA SecretStateHandler handle, data = {}", state.data());

        // 1) сериализация состояния
        String json = state.data().toString();

//        // 2) простейший коалесинг (не пишем, если значение не изменилось)
//        String prev = lastJson.getAndSet(json);
//        if (prev != null && prev.equals(json) && !initial) {
//            return; // нечего обновлять в секрете
//        }

        // 3) асинхронная запись секрета
        k8sWrites.submit(() -> {
            try {
                k8s.createOrUpdateSecret(SECRET_NAME, namespace, json, Collections.emptyMap());
            }
            catch (Exception e) {
                log.error("VLLA SecretStateHandler error", e);
            }
        });
    }
}
