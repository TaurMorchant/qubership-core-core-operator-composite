package com.netcracker.core.declarative.client.k8s;

import com.netcracker.core.declarative.resources.maas.Maas;
import com.netcracker.core.declarative.resources.maas.MaasList;
import com.netcracker.core.declarative.resources.mesh.Mesh;
import com.netcracker.core.declarative.resources.mesh.MeshList;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

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
}
