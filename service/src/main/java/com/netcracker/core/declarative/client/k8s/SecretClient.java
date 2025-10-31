package com.netcracker.core.declarative.client.k8s;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.*;

@ApplicationScoped
@Slf4j
public class SecretClient {
    private final KubernetesClient client;
    private final String microserviceName;

    @Inject
    public SecretClient(KubernetesClient client,
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

        log.debug("Start creating or updating secret with name = {}", name);

        Secret existingSecret = client.secrets()
                .inNamespace(namespace)
                .withName(name)
                .get();

        Map<String, String> effectiveLabels = resolveSecretLabels(existingSecret, labels);

        SecretBuilder.MetadataNested<SecretBuilder> metadataBuilder = new SecretBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
                .withLabels(effectiveLabels);

        List<OwnerReference> ownerReferences = resolveOwnerReferences(name, namespace, existingSecret);
        if (!ownerReferences.isEmpty()) {
            metadataBuilder.withOwnerReferences(ownerReferences);
        }

        Secret secret = metadataBuilder
                .endMetadata()
                .withType("Opaque")
                .withStringData(data)
                .build();

        client.secrets()
                .inNamespace(namespace)
                .resource(secret).serverSideApply();
    }

    private Map<String, String> resolveSecretLabels(Secret existingSecret,
                                                    Map<String, String> labels) {
        Map<String, String> mergedLabels = new HashMap<>();
        if (existingSecret != null
            && existingSecret.getMetadata() != null
            && existingSecret.getMetadata().getLabels() != null) {
            mergedLabels.putAll(existingSecret.getMetadata().getLabels());
        }

        mergedLabels.put("app.kubernetes.io/part-of", "Cloud-Core");
        mergedLabels.put("app.kubernetes.io/managed-by", "core-operator");

        if (labels != null) {
            mergedLabels.putAll(labels);
        }

        return mergedLabels;
    }

    private List<OwnerReference> resolveOwnerReferences(String secretName,
                                                        String namespace,
                                                        Secret existingSecret) {
        OwnerReference ownerReference = resolveDeploymentOwnerReference(namespace);
        if (ownerReference != null) {
            return List.of(ownerReference);
        }

        if (existingSecret != null
            && existingSecret.getMetadata() != null
            && existingSecret.getMetadata().getOwnerReferences() != null
            && !existingSecret.getMetadata().getOwnerReferences().isEmpty()) {
            log.debug("Deployment owner reference is unavailable. Keeping existing owner references for secret '{}'", secretName);
            return existingSecret.getMetadata().getOwnerReferences();
        }

        log.warn("Unable to resolve owner reference for secret '{}' in namespace '{}'", secretName, namespace);
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
