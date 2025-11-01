package com.netcracker.core.declarative.client.k8s;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.*;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SecretClientTest {

    private static final String SECRET_NAME = "test-secret";
    private static final String NAMESPACE = "test-namespace";
    private static final String MICRO_NAME = "microservice";

    @Test
    void shouldMergeLabelsAndSetOwnerReferenceFromDeployment() {
        KubernetesClient client = mock(KubernetesClient.class);
        Secret existingSecret = new SecretBuilder()
                .withNewMetadata()
                .withLabels(Map.of("existing", "label"))
                .endMetadata()
                .build();

        AtomicReference<Secret> appliedSecretRef = prepareSecretMocks(client, existingSecret);
        stubDeployment(client, createDeployment("uid-123"));

        SecretClient secretClient = new SecretClient(client, MICRO_NAME);
        Map<String, String> data = Map.of("key", "value");
        Map<String, String> labels = Map.of("custom", "label");

        secretClient.createOrUpdate(SECRET_NAME, NAMESPACE, data, labels);

        Secret appliedSecret = appliedSecretRef.get();
        assertNotNull(appliedSecret);
        assertEquals(data, appliedSecret.getStringData());

        Map<String, String> resultingLabels = appliedSecret.getMetadata().getLabels();
        assertEquals(4, resultingLabels.size());
        assertEquals("label", resultingLabels.get("existing"));
        assertEquals("Cloud-Core", resultingLabels.get("app.kubernetes.io/part-of"));
        assertEquals("core-operator", resultingLabels.get("app.kubernetes.io/managed-by"));
        assertEquals("label", resultingLabels.get("custom"));

        List<OwnerReference> ownerReferences = appliedSecret.getMetadata().getOwnerReferences();
        assertEquals(1, ownerReferences.size());
        OwnerReference ownerReference = ownerReferences.getFirst();
        assertEquals("apps/v1", ownerReference.getApiVersion());
        assertEquals("Deployment", ownerReference.getKind());
        assertEquals(MICRO_NAME, ownerReference.getName());
        assertEquals("uid-123", ownerReference.getUid());
    }

    @Test
    void shouldReuseExistingOwnerReferencesWhenDeploymentUnavailable() {
        KubernetesClient client = mock(KubernetesClient.class);
        OwnerReference existingOwner = new OwnerReferenceBuilder()
                .withApiVersion("apps/v1")
                .withKind("Deployment")
                .withName("existing")
                .withUid("uid-001")
                .build();
        Secret existingSecret = new SecretBuilder()
                .withNewMetadata()
                .withOwnerReferences(existingOwner)
                .endMetadata()
                .build();

        AtomicReference<Secret> appliedSecretRef = prepareSecretMocks(client, existingSecret);
        stubDeployment(client, null);

        SecretClient secretClient = new SecretClient(client, MICRO_NAME);
        secretClient.createOrUpdate(SECRET_NAME, NAMESPACE, Map.of("key", "value"), Map.of());

        Secret appliedSecret = appliedSecretRef.get();
        assertNotNull(appliedSecret);
        List<OwnerReference> ownerReferences = appliedSecret.getMetadata().getOwnerReferences();
        assertEquals(1, ownerReferences.size());
        assertEquals(existingOwner, ownerReferences.getFirst());
    }

    @Test
    void shouldLeaveOwnerReferencesEmptyWhenNotAvailable() {
        KubernetesClient client = mock(KubernetesClient.class);
        AtomicReference<Secret> appliedSecretRef = prepareSecretMocks(client, null);
        stubDeployment(client, createDeployment(null));

        SecretClient secretClient = new SecretClient(client, MICRO_NAME);
        secretClient.createOrUpdate(SECRET_NAME, NAMESPACE, Map.of("key", "value"), null);

        Secret appliedSecret = appliedSecretRef.get();
        assertNotNull(appliedSecret);
        assertEquals(Collections.emptyList(), appliedSecret.getMetadata().getOwnerReferences());
    }

    private AtomicReference<Secret> prepareSecretMocks(KubernetesClient client, Secret existingSecret) {
        @SuppressWarnings("unchecked")
        MixedOperation<Secret, SecretList, Resource<Secret>> secretOperation = mock(MixedOperation.class);
        @SuppressWarnings("unchecked")
        NonNamespaceOperation<Secret, SecretList, Resource<Secret>> namespacedSecretOperation = mock(NonNamespaceOperation.class);
        @SuppressWarnings("unchecked")
        Resource<Secret> namedSecretResource = mock(Resource.class);
        @SuppressWarnings("unchecked")
        Resource<Secret> secretResource = mock(Resource.class);

        when(client.secrets()).thenReturn(secretOperation);
        when(secretOperation.inNamespace(NAMESPACE)).thenReturn(namespacedSecretOperation);
        when(namespacedSecretOperation.withName(SECRET_NAME)).thenReturn(namedSecretResource);
        when(namedSecretResource.get()).thenReturn(existingSecret);

        AtomicReference<Secret> appliedSecretRef = new AtomicReference<>();
        when(namespacedSecretOperation.resource(Mockito.any(Secret.class))).thenAnswer(invocation -> {
            Secret secret = invocation.getArgument(0);
            appliedSecretRef.set(secret);
            return secretResource;
        });
        when(secretResource.serverSideApply()).thenReturn(null);

        return appliedSecretRef;
    }

    private void stubDeployment(KubernetesClient client, Deployment deployment) {
        AppsAPIGroupDSL apps = mock(AppsAPIGroupDSL.class);
        @SuppressWarnings("unchecked")
        MixedOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> deploymentOperation = mock(MixedOperation.class);
        @SuppressWarnings("unchecked")
        NonNamespaceOperation<Deployment, DeploymentList, RollableScalableResource<Deployment>> namespacedDeploymentOperation = mock(NonNamespaceOperation.class);
        @SuppressWarnings("unchecked")
        RollableScalableResource<Deployment> deploymentResource = mock(RollableScalableResource.class);

        when(client.apps()).thenReturn(apps);
        when(apps.deployments()).thenReturn(deploymentOperation);
        when(deploymentOperation.inNamespace(NAMESPACE)).thenReturn(namespacedDeploymentOperation);
        when(namespacedDeploymentOperation.withName(MICRO_NAME)).thenReturn(deploymentResource);
        when(deploymentResource.get()).thenReturn(deployment);
    }

    private Deployment createDeployment(String uid) {
        Deployment deployment = new Deployment();
        ObjectMeta metadata = new ObjectMeta();
        metadata.setName(MICRO_NAME);
        metadata.setUid(uid);
        deployment.setMetadata(metadata);
        return deployment;
    }
}
