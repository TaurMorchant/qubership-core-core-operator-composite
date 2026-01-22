package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.service.composite.consul.ConsulSnapshotHandler;
import com.netcracker.core.declarative.service.composite.consul.model.CompositeStructureConfigMapPayload;
import com.netcracker.core.declarative.service.composite.consul.model.CompositeStructure;
import com.netcracker.core.declarative.service.composite.consul.model.CompositeStructureSerializer;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulSnapshotSerializationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Map;

import static com.netcracker.core.declarative.service.composite.CompositeStructureWatchCoordinator.CONFIG_MAP_NAME;

/**
 * Transforms Consul composite structure snapshots into ConfigMap.
 * <p>
 * Enriches the structure with cloud provider metadata and serializes to JSON
 * for storage in the {@code composite-structure} ConfigMap.
 */
@ApplicationScoped
@Slf4j
public class CompositeStructureSnapshotHandler implements ConsulSnapshotHandler {
    private static final String CONFIG_MAP_DATA_KEY = "data";
    private static final String DEFAULT_CLOUD_PROVIDER = "OnPrem";

    private final ObjectMapper objectMapper;
    private final String cloudProvider;
    private final ConfigMapWriter configMapWriter;

    @Inject
    public CompositeStructureSnapshotHandler(ObjectMapper objectMapper,
                                             ConfigMapWriter configMapWriter,
                                             @ConfigProperty(name = "CLOUD_PROVIDER", defaultValue = DEFAULT_CLOUD_PROVIDER) String cloudProvider) {
        this.objectMapper = objectMapper;
        this.configMapWriter = configMapWriter;
        this.cloudProvider = cloudProvider;
    }

    @Override
    public void handle(ConsulPrefixSnapshot compositeStructureSnapshot) {
        log.info("Store Composite Structure to config map {}", CONFIG_MAP_NAME);
        try {
            CompositeStructure compositePayload = CompositeStructureSerializer.toPayload(compositeStructureSnapshot);
            CompositeStructureConfigMapPayload payload = new CompositeStructureConfigMapPayload(
                    cloudProvider,
                    compositePayload
            );
            String json = objectMapper.writeValueAsString(payload);
            Map<String, String> compositeStructureContent = Map.of(CONFIG_MAP_DATA_KEY, json);
            configMapWriter.requestUpdate(CONFIG_MAP_NAME, compositeStructureContent);
        } catch (JsonProcessingException e) {
            throw new ConsulSnapshotSerializationException("Failed to serialize composite structure to JSON", e);
        }
    }
}
