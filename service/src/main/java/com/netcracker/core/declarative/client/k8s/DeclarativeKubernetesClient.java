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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class DeclarativeKubernetesClient {

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
                                     String compositeJson,
                                     Map<String, String> labels) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(compositeJson, "compositeJson");

        log.info("VLLA createOrUpdateSecret name = {}", name);

        Map<String, String> effectiveLabels = (labels == null) ? Collections.emptyMap() : labels;

        Secret secret = new SecretBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
                .withLabels(effectiveLabels)
                .endMetadata()
                .withType("Opaque")
                .withStringData(Map.of("compositeStructure", compositeJson))
                .build();

        client.secrets()
                .inNamespace(namespace)
                .resource(secret).serverSideApply();
    }
}
