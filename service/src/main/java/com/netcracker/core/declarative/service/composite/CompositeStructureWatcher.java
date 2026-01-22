package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.service.composite.consul.ConsulClient;
import com.netcracker.core.declarative.service.composite.consul.ConsulSnapshotHandler;
import com.netcracker.core.declarative.service.composite.consul.longpoll.ConsulLongPoller;
import com.netcracker.core.declarative.service.composite.consul.longpoll.ConsulLongPollerFactory;
import com.netcracker.core.declarative.service.composite.consul.longpoll.LongPollConfig;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Watches Consul for composite structure changes using long-polling.
 * <p>
 * Monitors two Consul paths:
 * <ol>
 *   <li>{@code config/{namespace}/application/composite/structureRef} - reference to the current structure prefix</li>
 *   <li>The prefix itself - actual composite structure data</li>
 * </ol>
 * When the structure changes, delegates handling to {@link ConsulSnapshotHandler}.
 */
@Slf4j
public class CompositeStructureWatcher {

    private static final String COMPOSITE_STRUCTURE_REF_TEMPLATE = "config/%s/application/composite/structureRef";
    private static final LongPollConfig KV_POLL_CONFIG = LongPollConfig.builder()
            .wait(Duration.ofMinutes(9))
            .build();

    private final String compositeStructureRefKey;
    private final ConsulClient consulClient;
    private final ConsulSnapshotHandler consulSnapshotHandler;
    private final ConsulLongPollerFactory pollerFactory;
    private final Object lock = new Object();

    private ConsulLongPoller compositeStructureRefPoller;
    private ConsulLongPoller compositeStructurePoller;
    private String currentCompositeStructureConsulPrefix;
    private boolean stopped = false;

    public CompositeStructureWatcher(String namespace,
                                     ConsulClient consulClient,
                                     ConsulSnapshotHandler compositeStructureStateHandler) {
        this(namespace, consulClient, compositeStructureStateHandler, ConsulLongPollerFactory.defaultFactory());
    }

    CompositeStructureWatcher(String namespace,
                              ConsulClient consulClient,
                              ConsulSnapshotHandler consulSnapshotHandler,
                              ConsulLongPollerFactory pollerFactory) {
        this.compositeStructureRefKey = COMPOSITE_STRUCTURE_REF_TEMPLATE.formatted(namespace);
        this.consulClient = consulClient;
        this.consulSnapshotHandler = consulSnapshotHandler;
        this.pollerFactory = pollerFactory;
    }

    void start() {
        synchronized (lock) {
            if (compositeStructureRefPoller != null) {
                log.debug("CompositeWatcher already started, skipping");
                return;
            }
            stopped = false;
            log.info("CompositeWatcher start. Composite Structure ref key = '{}'", compositeStructureRefKey);

            compositeStructureRefPoller = createPoller(compositeStructureRefKey, this::onCompositeStructureRefSnapshot);
            compositeStructureRefPoller.start();
        }
    }

    void stop() {
        synchronized (lock) {
            stopped = true;
            if (compositeStructureRefPoller != null) {
                compositeStructureRefPoller.close();
                compositeStructureRefPoller = null;
            }
            if (compositeStructurePoller != null) {
                compositeStructurePoller.close();
                compositeStructurePoller = null;
            }
            currentCompositeStructureConsulPrefix = null;
            log.info("CompositeWatcher stopped.");
        }
    }

    private void onCompositeStructureRefSnapshot(ConsulPrefixSnapshot snapshot) {
        final String compositeStructurePrefix = snapshot.getValue(compositeStructureRefKey);
        log.info("Current composite structure prefix = '{}'", compositeStructurePrefix);
        switchCompositeStructurePoller(compositeStructurePrefix);
    }

    private void switchCompositeStructurePoller(String newCompositeStructureConsulPrefix) {
        synchronized (lock) {
            if (stopped) {
                return;
            }
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

            compositeStructurePoller = createPoller(newCompositeStructureConsulPrefix, consulSnapshotHandler::handle);
            compositeStructurePoller.start();
        }
    }

    private ConsulLongPoller createPoller(String path, Consumer<ConsulPrefixSnapshot> onSnapshot) {
        return pollerFactory.create(path, consulClient, KV_POLL_CONFIG, onSnapshot);
    }
}
