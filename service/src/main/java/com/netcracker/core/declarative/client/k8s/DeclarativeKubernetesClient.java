package com.netcracker.core.declarative.client.k8s;

import com.netcracker.core.declarative.resources.maas.Maas;
import com.netcracker.core.declarative.resources.maas.MaasList;
import com.netcracker.core.declarative.resources.mesh.Mesh;
import com.netcracker.core.declarative.resources.mesh.MeshList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class DeclarativeKubernetesClient {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss");

    final KubernetesClient client;

    public DeclarativeKubernetesClient(KubernetesClient client) {
        this.client = client;
    }

    public KubernetesClient getRawClient() {
        return client;
    }

    public MixedOperation<Mesh, MeshList, Resource<Mesh>> mesh() {
        return client.resources(Mesh.class, MeshList.class);
    }

    public MixedOperation<Maas, MaasList, Resource<Maas>> maas() {
        return client.resources(Maas.class, MaasList.class);
    }

    public void createOrUpdateSecret(String name,
                                     String namespace,
                                     Map<String, String> data,
                                     Map<String, String> labels) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(namespace, "namespace");

        log.debug("Start creating or updating secret with name = {}", name);

        Map<String, String> effectiveLabels = resolveSecretLabels(name, namespace, labels);

        Secret secret = new SecretBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
                .withLabels(effectiveLabels)
                .endMetadata()
                .withType("Opaque")
                .withStringData(data)
                .build();

        client.secrets()
                .inNamespace(namespace)
                .resource(secret).serverSideApply();
    }

    private Map<String, String> resolveSecretLabels(String name,
                                                    String namespace,
                                                    Map<String, String> labels) {
        Secret existingSecret = client.secrets()
                .inNamespace(namespace)
                .withName(name)
                .get();

        Map<String, String> mergedLabels = new HashMap<>();
        if (existingSecret != null
                && existingSecret.getMetadata() != null
                && existingSecret.getMetadata().getLabels() != null) {
            mergedLabels.putAll(existingSecret.getMetadata().getLabels());
        }

        mergedLabels.put("app.kubernetes.io/part-of", "Cloud-Core");
        mergedLabels.put("app.kubernetes.io/managed-by", "core-operator");
        mergedLabels.put("modified-at", LocalDateTime.now().format(FORMATTER));

        if (labels != null) {
            mergedLabels.putAll(labels);
        }

        return mergedLabels;
    }
}
