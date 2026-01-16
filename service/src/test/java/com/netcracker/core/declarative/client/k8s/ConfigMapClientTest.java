package com.netcracker.core.declarative.client.k8s;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
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

class ConfigMapClientTest {

    private static final String CONFIG_MAP_NAME = "test-config-map";
    private static final String NAMESPACE = "test-namespace";
    private static final String MICRO_NAME = "microservice";

    @Test
    void shouldMergeLabelsAndSetOwnerReferenceFromDeployment() {
        KubernetesClient client = mock(KubernetesClient.class);
        ConfigMap existingConfigMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withLabels(Map.of("existing", "label"))
                .endMetadata()
                .build();

        AtomicReference<ConfigMap> appliedConfigMapRef = prepareConfigMapMocks(client, existingConfigMap);
        stubDeployment(client, createDeployment("uid-123"));

        ConfigMapClient configMapClient = new ConfigMapClient(client, MICRO_NAME);
        Map<String, String> data = Map.of("key", "value");
        Map<String, String> labels = Map.of("custom", "label");

        configMapClient.createOrUpdate(CONFIG_MAP_NAME, NAMESPACE, data, labels);

        ConfigMap appliedConfigMap = appliedConfigMapRef.get();
        assertNotNull(appliedConfigMap);
        assertEquals(data, appliedConfigMap.getData());

        Map<String, String> resultingLabels = appliedConfigMap.getMetadata().getLabels();
        assertEquals(4, resultingLabels.size());
        assertEquals("label", resultingLabels.get("existing"));
        assertEquals("Cloud-Core", resultingLabels.get("app.kubernetes.io/part-of"));
        assertEquals("core-operator", resultingLabels.get("app.kubernetes.io/managed-by"));
        assertEquals("label", resultingLabels.get("custom"));

        List<OwnerReference> ownerReferences = appliedConfigMap.getMetadata().getOwnerReferences();
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
        ConfigMap existingConfigMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withOwnerReferences(existingOwner)
                .endMetadata()
                .build();

        AtomicReference<ConfigMap> appliedConfigMapRef = prepareConfigMapMocks(client, existingConfigMap);
        stubDeployment(client, null);

        ConfigMapClient configMapClient = new ConfigMapClient(client, MICRO_NAME);
        configMapClient.createOrUpdate(CONFIG_MAP_NAME, NAMESPACE, Map.of("key", "value"), Map.of());

        ConfigMap appliedConfigMap = appliedConfigMapRef.get();
        assertNotNull(appliedConfigMap);
        List<OwnerReference> ownerReferences = appliedConfigMap.getMetadata().getOwnerReferences();
        assertEquals(1, ownerReferences.size());
        assertEquals(existingOwner, ownerReferences.getFirst());
    }

    @Test
    void shouldLeaveOwnerReferencesEmptyWhenNotAvailable() {
        KubernetesClient client = mock(KubernetesClient.class);
        AtomicReference<ConfigMap> appliedConfigMapRef = prepareConfigMapMocks(client, null);
        stubDeployment(client, createDeployment(null));

        ConfigMapClient configMapClient = new ConfigMapClient(client, MICRO_NAME);
        configMapClient.createOrUpdate(CONFIG_MAP_NAME, NAMESPACE, Map.of("key", "value"), null);

        ConfigMap appliedConfigMap = appliedConfigMapRef.get();
        assertNotNull(appliedConfigMap);
        assertEquals(Collections.emptyList(), appliedConfigMap.getMetadata().getOwnerReferences());
    }

    private AtomicReference<ConfigMap> prepareConfigMapMocks(KubernetesClient client, ConfigMap existingConfigMap) {
        @SuppressWarnings("unchecked")
        MixedOperation<ConfigMap, ConfigMapList, Resource<ConfigMap>> configMapOperation = mock(MixedOperation.class);
        @SuppressWarnings("unchecked")
        NonNamespaceOperation<ConfigMap, ConfigMapList, Resource<ConfigMap>> namespacedConfigMapOperation = mock(NonNamespaceOperation.class);
        @SuppressWarnings("unchecked")
        Resource<ConfigMap> namedConfigMapResource = mock(Resource.class);
        @SuppressWarnings("unchecked")
        Resource<ConfigMap> configMapResource = mock(Resource.class);

        when(client.configMaps()).thenReturn(configMapOperation);
        when(configMapOperation.inNamespace(NAMESPACE)).thenReturn(namespacedConfigMapOperation);
        when(namespacedConfigMapOperation.withName(CONFIG_MAP_NAME)).thenReturn(namedConfigMapResource);
        when(namedConfigMapResource.get()).thenReturn(existingConfigMap);

        AtomicReference<ConfigMap> appliedConfigMapRef = new AtomicReference<>();
        when(namespacedConfigMapOperation.resource(Mockito.any(ConfigMap.class))).thenAnswer(invocation -> {
            ConfigMap configMap = invocation.getArgument(0);
            appliedConfigMapRef.set(configMap);
            return configMapResource;
        });
        when(configMapResource.serverSideApply()).thenReturn(null);

        return appliedConfigMapRef;
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
