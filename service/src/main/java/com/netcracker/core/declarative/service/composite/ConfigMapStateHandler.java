package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.DeclarativeKubernetesClient;
import com.netcracker.core.declarative.service.kv.KvLongPoller;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class ConfigMapStateHandler implements StructureStateHandler {
    private final DeclarativeKubernetesClient k8s;
    private final String namespace;
    private final Vertx vertx;
    private final WorkerExecutor k8sWrites; // общий пул для всех K8s-записей
    private final AtomicReference<String> lastJson = new AtomicReference<>();

    private static final String CM_NAME = "VLLA-TEST-CONFIG-MAP";

    @Inject
    public ConfigMapStateHandler(KubernetesClient client,
                                 Vertx vertx,
                                 @ConfigProperty(name = "cloud.microservice.namespace") String namespace) {
        this.k8s = new DeclarativeKubernetesClient(client);
        this.vertx = vertx;
        this.namespace = namespace;
        this.k8sWrites = vertx.createSharedWorkerExecutor("k8s-writes", 4, 60_000);
    }

    @Override
    public void handle(StructureState state, KvLongPoller.IndexPair idx, boolean initial) {
        // 1) сериализация состояния
        String json = state.data().toString();

        // 2) простейший коалесинг (не пишем, если значение не изменилось)
        String prev = lastJson.getAndSet(json);
        if (prev != null && prev.equals(json) && !initial) {
            return; // ничего не менять в конфиг-мапе
        }

        // 3) запись в K8s — ТОЛЬКО из worker, не из event loop
        vertx.executeBlocking(promise -> {
            try {
                k8s.createOrUpdateConfigMap(CM_NAME, namespace, json, Collections.emptyMap());
                promise.complete();
            } catch (Throwable t) {
                promise.fail(t);
            }
        }, false, ar -> {
            if (ar.failed()) {
                // не блокируем event loop повторными попытками — просто лог
                // (ретраи/бэк-офф можно делать выше по стеку)
                System.getLogger(getClass().getName())
                        .log(System.Logger.Level.WARNING,
                                "Failed to upsert ConfigMap {0} in ns {1}: {2}",
                                CM_NAME, namespace, ar.cause().toString());
            }
        });
    }
}
