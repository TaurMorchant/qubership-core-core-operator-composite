package com.netcracker.core.declarative.client.k8s;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@ApplicationScoped
@Slf4j
public class ConfigMapClient {
    public static final String LABEL_PART_OF = "app.kubernetes.io/part-of";
    public static final String LABEL_MANAGED_BY = "app.kubernetes.io/managed-by";
    public static final String MANAGED_BY_CORE_OPERATOR = "core-operator";
    public static final String MANAGED_BY_TOPOLOGY_OPERATOR = "topology-operator";

    private final KubernetesClient client;

    @Inject
    public ConfigMapClient(KubernetesClient client) {
        this.client = client;
    }

    public void createOrUpdate(String name,
                               String namespace,
                               Map<String, String> data,
                               Map<String, String> labels) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(namespace, "namespace");

        log.debug("Start creating or updating config map with name = {}", name);

        ConfigMap existingConfigMap = client.configMaps()
                .inNamespace(namespace)
                .withName(name)
                .get();

        if (!isManagedByCoreOperator(existingConfigMap)) {
            log.info("Config map '{}' in namespace '{}' is managed by '{}'. Skipping update.",
                    name, namespace, MANAGED_BY_TOPOLOGY_OPERATOR);
            return;
        }

        Map<String, String> effectiveLabels = resolveConfigMapLabels(existingConfigMap, labels);

        ConfigMapBuilder.MetadataNested<ConfigMapBuilder> metadataBuilder = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
                .withLabels(effectiveLabels);

        ConfigMap configMap = metadataBuilder
                .endMetadata()
                .withData(data)
                .build();

        client.configMaps()
                .inNamespace(namespace)
                .resource(configMap).serverSideApply();
    }

    private Map<String, String> resolveConfigMapLabels(ConfigMap existingConfigMap,
                                                       Map<String, String> labels) {
        Map<String, String> mergedLabels = new HashMap<>();
        if (existingConfigMap != null
            && existingConfigMap.getMetadata() != null
            && existingConfigMap.getMetadata().getLabels() != null) {
            mergedLabels.putAll(existingConfigMap.getMetadata().getLabels());
        }

        mergedLabels.put(LABEL_PART_OF, "Cloud-Core");
        mergedLabels.put(LABEL_MANAGED_BY, MANAGED_BY_CORE_OPERATOR);

        if (labels != null) {
            mergedLabels.putAll(labels);
        }

        return mergedLabels;
    }

    public boolean isManagedByCoreOperator(String name, String namespace) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(namespace, "namespace");

        ConfigMap existingConfigMap = client.configMaps()
                .inNamespace(namespace)
                .withName(name)
                .get();

        return isManagedByCoreOperator(existingConfigMap);
    }

    boolean isManagedByCoreOperator(ConfigMap existingConfigMap) {
        if (existingConfigMap == null || existingConfigMap.getMetadata() == null) {
            return true;
        }

        Map<String, String> labels = existingConfigMap.getMetadata().getLabels();
        if (labels == null) {
            return true;
        }

        return !MANAGED_BY_TOPOLOGY_OPERATOR.equals(labels.get(LABEL_MANAGED_BY));
    }
}
