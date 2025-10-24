package com.netcracker.core.declarative.service;

import io.vertx.ext.consul.*;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import com.netcracker.cloud.consul.provider.common.TokenStorage;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
@AllArgsConstructor
public class CompositeConsulUpdaterImpl implements CompositeConsulUpdater {
    private static final String CONTROLLER_NAMESPACE_KEY = "controllerNamespace";
    private static final String COMPOSITE_STRUCTURE_BASE_PATH_TEMPLATE = "composite/%s/structure";
    private static final String COMPOSITE_STRUCTURE_NAMESPACE_BASE_PATH_TEMPLATE = "composite/%s/structure/%s";
    private static final String COMPOSITE_ROLE_BASE_PATH_TEMPLATE = COMPOSITE_STRUCTURE_NAMESPACE_BASE_PATH_TEMPLATE + "/compositeRole";
    private static final String BLUE_GREEN_ROLE_BASE_PATH_TEMPLATE = COMPOSITE_STRUCTURE_NAMESPACE_BASE_PATH_TEMPLATE + "/bluegreenRole";
    private static final String CONTROLLER_NAMESPACE_BASE_PATH_TEMPLATE = COMPOSITE_STRUCTURE_NAMESPACE_BASE_PATH_TEMPLATE + "/" + CONTROLLER_NAMESPACE_KEY;
    private static final String COMPOSITE_REF_ROLE_BASE_PATH_TEMPLATE = "config/%s/application/composite/structureRef";

    private static final String BLUE_GREEN_ROLE_CONTROLLER = "controller";
    private static final String BLUE_GREEN_ROLE_ORIGIN = "origin";
    private static final String BLUE_GREEN_ROLE_PEER = "peer";

    private final String namespace;
    private final ConsulClientFactory consulClientFactory;
    private final TokenStorage consulTokenStorage;

    @Override
    public void updateCompositeStructureInConsul(CompositeSpec compositeSpec) throws ExecutionException, InterruptedException {
        updateCompositeStructureInConsul(namespace, compositeSpec);
    }

    @Override
    public void updateCompositeStructureInConsul(String namespace, CompositeSpec compositeSpec) throws ExecutionException, InterruptedException {
        boolean isBaseline = compositeSpec.isBaseline();
        String compositeId = compositeSpec.getCompositeId();
        String compositeDefinitionRoot = COMPOSITE_STRUCTURE_BASE_PATH_TEMPLATE.formatted(compositeId);

        TxnRequest request = new TxnRequest();
        cleanUp(compositeSpec.getCompositeId(), compositeSpec.getOriginNamespace()).forEach(request::addOperation);
        TxnKVOperation compositeRefOp = new TxnKVOperation()
                .setKey(COMPOSITE_REF_ROLE_BASE_PATH_TEMPLATE.formatted(namespace))
                .setValue(compositeDefinitionRoot)
                .setType(TxnKVVerb.SET);
        request.addOperation(compositeRefOp);
        boolean isBlueGreen = StringUtils.isNotEmpty(compositeSpec.getControllerNamespace());

        if (isBlueGreen) {
            // BC
            request.addOperation(writeBlueGreenRole(compositeId, compositeSpec.getControllerNamespace(), BLUE_GREEN_ROLE_CONTROLLER));
            request.addOperation(writeCompositeRole(compositeId, compositeSpec.getControllerNamespace(), isBaseline));

            // BO
            request.addOperation(writeBlueGreenRole(compositeId, compositeSpec.getOriginNamespace(), BLUE_GREEN_ROLE_ORIGIN));
            request.addOperation(writeCompositeRole(compositeId, compositeSpec.getOriginNamespace(), isBaseline));
            request.addOperation(writeControllerNamespace(compositeId, compositeSpec.getOriginNamespace(), compositeSpec.getControllerNamespace()));

            // BP
            request.addOperation(writeBlueGreenRole(compositeId, compositeSpec.getPeerNamespace(), BLUE_GREEN_ROLE_PEER));
            request.addOperation(writeCompositeRole(compositeId, compositeSpec.getPeerNamespace(), isBaseline));
            request.addOperation(writeControllerNamespace(compositeId, compositeSpec.getPeerNamespace(), compositeSpec.getControllerNamespace()));
        } else {
            request.addOperation(writeCompositeRole(compositeId, namespace, isBaseline));
        }

        log.info("Update composite structure in consul by path: {}", compositeDefinitionRoot);
        ConsulClient consulClient = consulClientFactory.create(consulTokenStorage.get());
        try {
            TxnResponse result = consulClient.transaction(request).toCompletionStage().toCompletableFuture().get();
            if (!result.getErrors().isEmpty()) {
                String errors = result.getErrors().stream().map(TxnError::getWhat).collect(Collectors.joining("\n"));
                log.error("Error update structure in consul: {}", errors);
                throw new RuntimeException("error update composite structure in consul: " + errors);
            }
        } finally {
            consulClient.close();
        }
    }

    @Override
    public Set<String> getCompositeMembers(String compositeId) throws ExecutionException, InterruptedException {
        String compositeDefinitionRoot = COMPOSITE_STRUCTURE_BASE_PATH_TEMPLATE.formatted(compositeId);
        log.info("Get updated composite structure from consul by path: {}", compositeDefinitionRoot);
        ConsulClient consulClient = consulClientFactory.create(consulTokenStorage.get());
        try {
            return consulClient.getKeys(compositeDefinitionRoot)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get()
                    .stream()
                    .map(s -> Paths.get(compositeDefinitionRoot).relativize(Paths.get(s)).getParent().toString())
                    .collect(Collectors.toSet());
        } finally {
            consulClient.close();
        }
    }

    private Set<TxnOperation> cleanUp(String compositeId, String originNamespace) throws ExecutionException, InterruptedException {
        ConsulClient consulClient = consulClientFactory.create(consulTokenStorage.get());
        try {
            List<KeyValue> struct = consulClient
                    .getValues(COMPOSITE_STRUCTURE_BASE_PATH_TEMPLATE.formatted(compositeId)).toCompletionStage().toCompletableFuture().get().getList();

            if (struct == null) {
                return Collections.emptySet();
            }
            Set<TxnOperation> toDelete = new HashSet<>();
            struct.stream()
                    .filter(keyValue -> keyValue.getKey().equals(COMPOSITE_ROLE_BASE_PATH_TEMPLATE.formatted(compositeId, originNamespace)))
                    .findFirst()
                    .ifPresent(keyValue -> toDelete.add(new TxnKVOperation()
                            .setKey(normalize(Paths.get(keyValue.getKey()).getParent().toString().replace("\\", "/")))
                            .setType(TxnKVVerb.DELETE_TREE)));

            struct.stream()
                    .filter(keyValue -> keyValue.getKey().equals(CONTROLLER_NAMESPACE_BASE_PATH_TEMPLATE.formatted(compositeId, originNamespace)))
                    .findFirst()
                    .ifPresent(value -> {
                                toDelete.add(new TxnKVOperation()
                                        .setKey(normalize(COMPOSITE_STRUCTURE_NAMESPACE_BASE_PATH_TEMPLATE.formatted(compositeId, value.getValue())))
                                        .setType(TxnKVVerb.DELETE_TREE));
                                toDelete.addAll(struct.stream()
                                        .filter(keyValue -> keyValue.getKey().endsWith(CONTROLLER_NAMESPACE_KEY))
                                        .filter(keyValue -> keyValue.getValue().equals(value.getValue()))
                                        .map(keyValue -> Paths.get(keyValue.getKey()).getParent().toString().replace("\\", "/"))
                                        .map(this::normalize)
                                        .map(p -> new TxnKVOperation().setKey(p).setType(TxnKVVerb.DELETE_TREE))
                                        .collect(Collectors.toSet()));
                            }
                    );
            log.info("CleanUp for compositeId '{}' and origin namespace '{}'; Will be deleted: {}",
                    compositeId, originNamespace, toDelete.stream().map(txnOperation -> ((TxnKVOperation) txnOperation).getKey()).collect(Collectors.toSet()));
            return toDelete;
        } finally {
            consulClient.close();
        }
    }

    private TxnOperation writeCompositeRole(String compositeId, String namespace, boolean isBaseline) {
        log.info("Adding composite role to Tx for compositeId '{}', namespace '{}', isBaseline '{}'", compositeId, namespace, isBaseline);
        return new TxnKVOperation()
                .setKey(COMPOSITE_ROLE_BASE_PATH_TEMPLATE.formatted(compositeId, namespace))
                .setValue(isBaseline ? "baseline" : "satellite")
                .setType(TxnKVVerb.SET);
    }

    private TxnOperation writeBlueGreenRole(String compositeId, String namespace, String blueGreenRole) {
        log.info("Adding blue-green role to Tx for compositeId '{}', namespace '{}', blue-green role '{}'", compositeId, namespace, blueGreenRole);
        return new TxnKVOperation()
                .setKey(BLUE_GREEN_ROLE_BASE_PATH_TEMPLATE.formatted(compositeId, namespace))
                .setValue(blueGreenRole)
                .setType(TxnKVVerb.SET);
    }

    private TxnOperation writeControllerNamespace(String compositeId, String namespace, String controllerNamespace) {
        log.info("Adding controller namespace to Tx for compositeId '{}', namespace '{}', controllerNamespace '{}'", compositeId, namespace, controllerNamespace);
        return new TxnKVOperation()
                .setKey(CONTROLLER_NAMESPACE_BASE_PATH_TEMPLATE.formatted(compositeId, namespace))
                .setValue(controllerNamespace)
                .setType(TxnKVVerb.SET);
    }

    private String normalize(String path) {
        if (!path.endsWith("/")) {
            return path + "/";
        }
        return path;
    }
}
