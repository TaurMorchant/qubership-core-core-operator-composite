package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static com.netcracker.core.declarative.service.composite.CompositeStructureWatchCoordinator.CONFIG_MAP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CompositeStructureSnapshotHandlerTest {

    private static final String CLOUD_PROVIDER = "OnPrem";
    private static final String CLOUD_OIDC_PROXY_URL = "http://super-proxy.namespace:8080";

    private ConfigMapWriter configMapUpdater;
    private CompositeStructureSnapshotHandler handler;

    @BeforeEach
    void setUp() {
        configMapUpdater = mock(ConfigMapWriter.class);
        handler = new CompositeStructureSnapshotHandler(
                new ObjectMapper(),
                configMapUpdater,
                CLOUD_PROVIDER,
                CLOUD_OIDC_PROXY_URL
        );
    }

    @Test
    void handleShouldSerializeSnapshotAndUpdateConfigMap() {
        ConsulPrefixSnapshot snapshot = snapshot(Map.of(
                "composite/sample/structure/ns-a/compositeRole", "baseline",
                "composite/sample/structure/ns-b/compositeRole", "satellite"
        ));

        handler.handle(snapshot);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> dataCaptor = ArgumentCaptor.forClass(Map.class);
        verify(configMapUpdater).requestUpdate(eq(CONFIG_MAP_NAME), dataCaptor.capture());

        Map<String, String> data = dataCaptor.getValue();
        assertEquals(1, data.size());
        assertEquals("{\"cloudProvider\":\"OnPrem\",\"cloudOIDCProxyUrl\":\"http://super-proxy.namespace:8080\","
                        + "\"composite\":{\"baseline\":{\"origin\":\"ns-a\"},\"satellites\":[{\"origin\":\"ns-b\"}]}}",
                data.get("data"));
    }

    private static ConsulPrefixSnapshot snapshot(Map<String, String> keyValues) {
        KeyValueList keyValueList = new KeyValueList();
        keyValueList.setList(keyValues.entrySet().stream()
                .map(entry -> new KeyValue().setKey(entry.getKey()).setValue(entry.getValue()))
                .toList());
        return new ConsulPrefixSnapshot(keyValueList);
    }

}
