package com.netcracker.core.declarative.client.k8s;

import com.netcracker.core.declarative.resources.maas.Maas;
import com.netcracker.core.declarative.resources.maas.MaasList;
import com.netcracker.core.declarative.resources.mesh.Mesh;
import com.netcracker.core.declarative.resources.mesh.MeshList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

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
}
