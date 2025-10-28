package com.netcracker.core.declarative.client.k8s;

import com.netcracker.core.declarative.resources.maas.Maas;
import com.netcracker.core.declarative.resources.maas.MaasList;
import com.netcracker.core.declarative.resources.mesh.Mesh;
import com.netcracker.core.declarative.resources.mesh.MeshList;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import lombok.extern.slf4j.Slf4j;

import java.util.Base64;
import java.util.HashMap;
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

    public void createOrUpdateConfigMap(String name,
                                        String namespace,
                                        String data,
                                        Map<String, String> labels) {
        log.info("VLLA createOrUpdateConfigMap data={}", data);
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .addToAnnotations("app.kubernetes.io/managed-by", "core-operator")
                .endMetadata()
                .addToData("compositeStructure", data)
                .build();

        ConfigMap result = client.configMaps()
                .inNamespace(namespace)
                .resource(cm)
                .fieldManager("core-operator")
                .serverSideApply();

        log.info("VLLA createOrUpdateConfigMap result = {}", result);
    }

    public void createOrUpdateSecret(String name,
                                     String namespace,
                                     String compositeJson,
                                     Map<String, String> labels) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(compositeJson, "compositeJson");

        Map<String, String> effectiveLabels = (labels == null) ? Map.of() : labels;

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


    /**
     * Чтение текущего JSON из секрета. Возвращает null, если секрета или ключа нет.
     */
    public String readCompositeJsonOrNull(String name, String namespace) {
        Secret s = client.secrets().inNamespace(namespace).withName(name).get();
        if (s == null || s.getData() == null) return null;
        String b64 = s.getData().get("compositeStructure");
        if (b64 == null) return null;
        return new String(Base64.getDecoder().decode(b64));
    }
}
