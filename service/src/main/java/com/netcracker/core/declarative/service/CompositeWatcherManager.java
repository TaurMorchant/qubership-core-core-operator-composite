package com.netcracker.core.declarative.service;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import com.netcracker.cloud.consul.provider.common.TokenStorage;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.List;

@Slf4j
public class CompositeWatcherManager {

    private static final String COMPOSITE_REF_ROLE_BASE_PATH_TEMPLATE = "config/%s/application/composite/structureRef";

    private final String namespace;
    private final ConsulClientFactory consulClientFactory;
    private final TokenStorage consulTokenStorage;

    private CompositeWatcher watcher;

    @Inject
    public CompositeWatcherManager(@ConfigProperty(name = "cloud.microservice.namespace") String namespace,
                                   @ConfigProperty(name = "quarkus.consul-source-config.agent.enabled") boolean consulEnabled,
                                   ConsulClientFactory consulClientFactory,
                                   TokenStorage consulTokenStorage) {
        //todo vlla hack
        if (consulEnabled) {
            this.namespace = namespace;
            this.consulClientFactory = consulClientFactory;
            this.consulTokenStorage = consulTokenStorage;
        }
        else {
            this.namespace = null;
            this.consulClientFactory = null;
            this.consulTokenStorage = null;
        }
    }

    void onStart(@Observes StartupEvent ev) {
        if (consulClientFactory != null) {
            ConsulClient client = consulClientFactory.create(consulTokenStorage.get());

            String prefix = COMPOSITE_REF_ROLE_BASE_PATH_TEMPLATE.formatted(namespace);
            log.debug("VLLA prefix = {}", prefix);
            watcher = new CompositeWatcher(client, prefix);
            watcher.start(this::handleChange);
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        if (watcher != null) {
            watcher.close();
        }
        log.info("CompositeWatcherManager stopped");
    }

    private void handleChange(KeyValueList kvs) {
        List<KeyValue> list = kvs.getList();
        long index = kvs.getIndex();
        log.info("Consul change detected (index={}, keys={})", index,
                list == null ? 0 : list.size());

        if (list != null) {
            for (KeyValue kv : list) {
                String key = kv.getKey();
                String val = kv.getValue();
                log.debug("VLLA KV: {} = {}", key, val);
            }
        }
    }
}
