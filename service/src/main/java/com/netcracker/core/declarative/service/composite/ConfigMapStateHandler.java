package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.DeclarativeKubernetesClient;
import com.netcracker.core.declarative.service.kv.KvLongPoller;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Collections;

@ApplicationScoped
public class ConfigMapStateHandler implements StructureStateHandler{
    DeclarativeKubernetesClient kubernetesClient;
    String namespace;

    @Inject
    public ConfigMapStateHandler(KubernetesClient client,
                                 @ConfigProperty(name = "cloud.microservice.namespace") String namespace) {
        this.kubernetesClient = new DeclarativeKubernetesClient(client);
        this.namespace = namespace;
    }

    @Override
    public void handle(StructureState state, KvLongPoller.IndexPair idx, boolean initial) {
        kubernetesClient.createOrUpdateConfigMap("VLLA-TEST-CONFIG-MAP", namespace, state.data().toString(), Collections.emptyMap());
    }
}
