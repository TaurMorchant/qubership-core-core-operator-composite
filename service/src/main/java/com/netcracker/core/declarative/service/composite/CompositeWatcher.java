package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.service.composite.consul.ConsulClient;
import com.netcracker.core.declarative.service.composite.consul.ConsulSnapshotHandler;
import com.netcracker.core.declarative.service.composite.consul.longpoll.ConsulLongPoller;
import com.netcracker.core.declarative.service.composite.consul.longpoll.LongPollConfig;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.util.Objects;

@ApplicationScoped
@Startup
@Slf4j
public class CompositeWatcher {

    private static final String COMPOSITE_STRUCTURE_REF_TEMPLATE = "config/%s/application/composite/structureRef";
    private static final LongPollConfig KV_POLL_CONFIG = LongPollConfig.builder()
            .wait(Duration.ofMinutes(9))
            .build();

    private final String compositeStructureRefKey;
    private final ConsulClient consulClient;
    private final ConsulSnapshotHandler consulSnapshotHandler;

    private ConsulLongPoller compositeStructureRefPoller;
    private ConsulLongPoller compositeStructurePoller;

    private volatile String currentCompositeStructureConsulPrefix;

    public CompositeWatcher(@ConfigProperty(name = "cloud.microservice.namespace") String namespace,
                            ConsulClient consulClient,
                            CompositeStructureToConfigMapHandler compositeStructureStateHandler) {
        this.compositeStructureRefKey = COMPOSITE_STRUCTURE_REF_TEMPLATE.formatted(namespace);
        this.consulClient = consulClient;
        this.consulSnapshotHandler = compositeStructureStateHandler;
    }

    @PostConstruct
    void start() {
        startCompositeStructureRefLongPoll();
    }

    void startCompositeStructureRefLongPoll() {
        log.info("CompositeWatcher start. Composite Structure ref key = '{}'", compositeStructureRefKey);

        this.compositeStructureRefPoller = ConsulLongPoller.builder()
                .path(compositeStructureRefKey)
                .consulClient(consulClient)
                .pollConfig(KV_POLL_CONFIG)
                .onSnapshot(this::onCompositeStructureRefSnapshot)
                .build();
        compositeStructureRefPoller.start();
    }

    @PreDestroy
    void stop() {
        if (compositeStructureRefPoller != null) {
            compositeStructureRefPoller.close();
        }
        if (compositeStructurePoller != null) {
            compositeStructurePoller.close();
        }
        log.info("CompositeWatcher stopped.");
    }

    private void onCompositeStructureRefSnapshot(ConsulPrefixSnapshot snapshot) {
        final String compositeStructurePrefix = snapshot.getValue(compositeStructureRefKey);
        log.info("Current composite structure prefix = '{}'", compositeStructurePrefix);
        startWatchCompositeStructure(compositeStructurePrefix);
    }

    private void startWatchCompositeStructure(String newCompositeStructureConsulPrefix) {
        if (Objects.equals(currentCompositeStructureConsulPrefix, newCompositeStructureConsulPrefix)) {
            log.debug("Composite Structure Consul prefix is unchanged: '{}'", newCompositeStructureConsulPrefix);
            return;
        }
        if (compositeStructurePoller != null) {
            compositeStructurePoller.close();
            compositeStructurePoller = null;
        }
        currentCompositeStructureConsulPrefix = newCompositeStructureConsulPrefix;

        if (newCompositeStructureConsulPrefix == null || newCompositeStructureConsulPrefix.isBlank()) {
            log.warn("structureRef empty — structure polling paused.");
            return;
        }

        log.info("Switching composite structure polling to prefix = '{}'", newCompositeStructureConsulPrefix);

        this.compositeStructurePoller = ConsulLongPoller.builder()
                .path(newCompositeStructureConsulPrefix)
                .consulClient(consulClient)
                .pollConfig(KV_POLL_CONFIG)
                .onSnapshot(consulSnapshotHandler::handle)
                .build();
        compositeStructurePoller.start();
    }
}
