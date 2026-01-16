package com.netcracker.core.declarative.client.k8s;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@ApplicationScoped
@Slf4j
public class ConfigMapClient {
    private final KubernetesClient client;
    private final String microserviceName;

    @Inject
    public ConfigMapClient(KubernetesClient client,
                           @ConfigProperty(name = "cloud.microservice.name") String microserviceName) {
        this.client = client;
        this.microserviceName = microserviceName;
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

        Map<String, String> effectiveLabels = resolveConfigMapLabels(existingConfigMap, labels);

        ConfigMapBuilder.MetadataNested<ConfigMapBuilder> metadataBuilder = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
                .withLabels(effectiveLabels);

        List<OwnerReference> ownerReferences = resolveOwnerReferences(name, namespace, existingConfigMap);
        if (!ownerReferences.isEmpty()) {
            metadataBuilder.withOwnerReferences(ownerReferences);
        }

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

        mergedLabels.put("app.kubernetes.io/part-of", "Cloud-Core");
        mergedLabels.put("app.kubernetes.io/managed-by", "core-operator");

        if (labels != null) {
            mergedLabels.putAll(labels);
        }

        return mergedLabels;
    }

    private List<OwnerReference> resolveOwnerReferences(String configMapName,
                                                        String namespace,
                                                        ConfigMap existingConfigMap) {
        OwnerReference ownerReference = resolveDeploymentOwnerReference(namespace);
        if (ownerReference != null) {
            return List.of(ownerReference);
        }

        if (existingConfigMap != null
            && existingConfigMap.getMetadata() != null
            && existingConfigMap.getMetadata().getOwnerReferences() != null
            && !existingConfigMap.getMetadata().getOwnerReferences().isEmpty()) {
            log.debug("Deployment owner reference is unavailable. Keeping existing owner references for config map '{}'", configMapName);
            return existingConfigMap.getMetadata().getOwnerReferences();
        }

        log.warn("Unable to resolve owner reference for config map '{}' in namespace '{}'", configMapName, namespace);
        return Collections.emptyList();
    }

    private OwnerReference resolveDeploymentOwnerReference(String namespace) {
        Deployment deployment = client
                .apps()
                .deployments()
                .inNamespace(namespace)
                .withName(microserviceName)
                .get();

        if (deployment == null || deployment.getMetadata() == null) {
            log.debug("Deployment '{}' not found in namespace '{}'.", microserviceName, namespace);
            return null;
        }

        String apiVersion = Objects.requireNonNullElse(deployment.getApiVersion(), "apps/v1");
        String kind = Objects.requireNonNullElse(deployment.getKind(), "Deployment");

        if (deployment.getMetadata().getUid() == null) {
            log.debug("Deployment '{}' in namespace '{}' does not have UID yet.", microserviceName, namespace);
            return null;
        }

        return new OwnerReferenceBuilder()
                .withApiVersion(apiVersion)
                .withKind(kind)
                .withName(deployment.getMetadata().getName())
                .withUid(deployment.getMetadata().getUid())
                .withController(true)
                .withBlockOwnerDeletion(true)
                .build();
    }
}
