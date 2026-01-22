package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulSnapshotSerializationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static com.netcracker.core.declarative.service.composite.CompositeStructureWatchCoordinator.CONFIG_MAP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class CompositeStructureSnapshotHandlerTest {

    private static final String CLOUD_PROVIDER = "OnPrem";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private ConfigMapWriter configMapWriter;
    private CompositeStructureSnapshotHandler handler;

    @BeforeEach
    void setUp() {
        configMapWriter = mock(ConfigMapWriter.class);
        handler = new CompositeStructureSnapshotHandler(
                objectMapper,
                configMapWriter,
                CLOUD_PROVIDER
        );
    }

    @Test
    void handleShouldSerializeSnapshotAndUpdateConfigMap() throws JsonProcessingException {
        ConsulPrefixSnapshot snapshot = createSnapshot(Map.of(
                "composite/sample/structure/ns-a/compositeRole", "baseline",
                "composite/sample/structure/ns-b/compositeRole", "satellite"
        ));

        handler.handle(snapshot);

        Map<String, String> capturedData = captureConfigMapData();
        String json = capturedData.get("data");
        assertNotNull(json);

        JsonNode root = objectMapper.readTree(json);
        assertEquals(CLOUD_PROVIDER, root.get("cloudProvider").asText());

        JsonNode composite = root.get("composite");
        assertNotNull(composite);
        assertEquals("ns-a", composite.get("baseline").get("origin").asText());
        assertEquals(1, composite.get("satellites").size());
        assertEquals("ns-b", composite.get("satellites").get(0).get("origin").asText());
    }

    @Test
    void handleShouldThrowOnSerializationError() {
        ConsulPrefixSnapshot snapshot = createSnapshot(Map.of(
                "composite/sample/structure/ns-a/compositeRole", "INVALID_ROLE"
        ));

        assertThrows(ConsulSnapshotSerializationException.class, () -> handler.handle(snapshot));

        verifyNoInteractions(configMapWriter);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> captureConfigMapData() {
        ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
        verify(configMapWriter).requestUpdate(eq(CONFIG_MAP_NAME), captor.capture());
        return captor.getValue();
    }

    private static ConsulPrefixSnapshot createSnapshot(Map<String, String> keyValues) {
        KeyValueList keyValueList = new KeyValueList();
        keyValueList.setList(keyValues.entrySet().stream()
                .map(entry -> new KeyValue().setKey(entry.getKey()).setValue(entry.getValue()))
                .toList());
        return new ConsulPrefixSnapshot(keyValueList);
    }
}
