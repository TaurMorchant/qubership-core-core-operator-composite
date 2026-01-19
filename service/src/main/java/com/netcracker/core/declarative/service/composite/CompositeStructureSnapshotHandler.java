package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.service.composite.consul.ConsulSnapshotHandler;
import com.netcracker.core.declarative.service.composite.consul.model.CompositeStructureConfigMapPayload;
import com.netcracker.core.declarative.service.composite.consul.model.CompositeStructurePayload;
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

@ApplicationScoped
@Slf4j
public class CompositeStructureSnapshotHandler implements ConsulSnapshotHandler {
    private static final String CONFIG_MAP_DATA_KEY = "data";
    private static final String DEFAULT_CLOUD_PROVIDER = "OnPrem";
    private static final String DEFAULT_CLOUD_OIDC_PROXY_URL = "http://super-proxy.namespace:8080";

    private final ObjectMapper objectMapper;
    private final String cloudProvider;
    private final String cloudOidcProxyUrl;
    private final ConfigMapWriter configMapWriter;

    @Inject
    public CompositeStructureSnapshotHandler(ObjectMapper objectMapper,
                                             ConfigMapWriter configMapWriter,
                                             @ConfigProperty(name = "CLOUD_PROVIDER", defaultValue = DEFAULT_CLOUD_PROVIDER) String cloudProvider,
                                             @ConfigProperty(name = "CLOUD_OIDC_PROXY_URL",
                                                        defaultValue = DEFAULT_CLOUD_OIDC_PROXY_URL) String cloudOidcProxyUrl) {
        this.objectMapper = objectMapper;
        this.configMapWriter = configMapWriter;
        this.cloudProvider = cloudProvider;
        this.cloudOidcProxyUrl = cloudOidcProxyUrl;
    }

    @Override
    public void handle(ConsulPrefixSnapshot compositeStructureSnapshot) {
        log.info("Store Composite Structure to config map {}", CONFIG_MAP_NAME);
        try {
            CompositeStructurePayload compositePayload = CompositeStructureSerializer.toPayload(compositeStructureSnapshot);
            CompositeStructureConfigMapPayload payload = new CompositeStructureConfigMapPayload(
                    cloudProvider,
                    cloudOidcProxyUrl,
                    compositePayload
            );
            String json = serializePayload(payload);
            Map<String, String> compositeStructureContent = Map.of(CONFIG_MAP_DATA_KEY, json);
            configMapWriter.requestUpdate(CONFIG_MAP_NAME, compositeStructureContent);
        } catch (ConsulSnapshotSerializationException | JsonProcessingException e) {
            log.error("Failed to serialize Consul snapshot for config map '{}'", CONFIG_MAP_NAME, e);
        }
    }

    private String serializePayload(CompositeStructureConfigMapPayload payload) throws JsonProcessingException {
        return objectMapper.writeValueAsString(payload);
    }
}
