package com.netcracker.core.declarative.service;

import com.netcracker.core.declarative.model.Tenant;
import io.vertx.ext.consul.*;
import jakarta.ws.rs.ServerErrorException;
import lombok.extern.slf4j.Slf4j;
import com.netcracker.cloud.consul.provider.common.TokenStorage;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public class TenantService {
    private final ConsulClientFactory consulClientFactory;
    private final TokenStorage consulTokenStorage;

    public TenantService(ConsulClientFactory consulClientFactory, TokenStorage consulTokenStorage) {
        this.consulClientFactory = consulClientFactory;
        this.consulTokenStorage = consulTokenStorage;
    }

    public void add(Tenant tenant) throws ExecutionException, InterruptedException {
        log.info("Add tenant: {}", tenant);
        if (tenant == null) {
            throw new IllegalArgumentException("tenant must be not null");
        }
        ConsulClient consulClient = consulClientFactory.create(consulTokenStorage.get());
        try {
            Optional<String> compositeId = getCompositeIdForMember(tenant.getNamespace());
            if (compositeId.isEmpty()) {
                String errMessage = "Composite id not found for namespace=%s".formatted(tenant.getNamespace());
                log.info(errMessage);
                throw new ServerErrorException(errMessage, 500);
            }
            log.info("Found composite_id={} for namespace={}", compositeId, tenant.getNamespace());
            String defaultTenantPath = "composite/%s/config/tenants/default".formatted(compositeId.get());
            TxnRequest txnRequest = new TxnRequest();
            txnRequest.addOperation(new TxnKVOperation()
                    .setKey("%s/id".formatted(defaultTenantPath))
                    .setValue(tenant.getTenantId())
                    .setType(TxnKVVerb.SET)
            );
            if (tenant.getDefaultTenantVars() != null && !tenant.getDefaultTenantVars().isEmpty()) {
                txnRequest.addOperation(new TxnKVOperation()
                        .setKey("%s/default_tenant_vars".formatted(defaultTenantPath))
                        .setValue(String.join(",", tenant.getDefaultTenantVars()))
                        .setType(TxnKVVerb.SET)
                );
            }
            TxnResponse txnResponse = consulClient.transaction(txnRequest).toCompletionStage().toCompletableFuture().get();
            if (txnResponse.getErrors() != null && !txnResponse.getErrors().isEmpty()) {
                var errors = "Error adding default tenant: %s".formatted(
                        txnResponse.getErrors().stream().map(TxnError::getWhat).collect(Collectors.joining("\n"))
                    );
                log.error(errors);
                throw new ServerErrorException(errors, 500);
            }
        } finally {
            consulClient.close();
        }
    }

    protected Optional<String> getCompositeIdForMember(String namespace) throws ExecutionException, InterruptedException {
        ConsulClient consulClient = consulClientFactory.create(consulTokenStorage.get());
        try {
            return consulClient.getKeys("composite/")
                    .map(keys -> getCompositeIdForMember(keys, namespace))
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get();
        } finally {
            consulClient.close();
        }
    }

    protected static Optional<String> getCompositeIdForMember(List<String> keys, String namespace) {
        return keys.stream()
                .map(path -> path.split("/"))
                .filter(chunks -> chunks.length > 2)
                .filter(chunks -> chunks[3].equals(namespace))
                .findFirst()
                .map(chunks -> chunks[1]);
    }

}
