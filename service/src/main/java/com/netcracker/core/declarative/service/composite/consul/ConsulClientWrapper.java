package com.netcracker.core.declarative.service.composite.consul;

import com.netcracker.cloud.consul.provider.common.TokenStorage;
import com.netcracker.core.declarative.service.ConsulClientFactory;
import com.netcracker.core.declarative.service.composite.consul.longpoll.PollResultHandler;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import io.vertx.core.AsyncResult;
import io.vertx.ext.consul.BlockingQueryOptions;
import io.vertx.ext.consul.KeyValueList;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@ApplicationScoped
@Slf4j
public final class ConsulClientWrapper implements ConsulClient {
    private final ConsulClientFactory consulClientFactory;
    private final TokenStorage tokenStorage;
    private final long readTimeoutMillis;

    @Inject
    public ConsulClientWrapper(ConsulClientFactory consulClientFactory,
                               Instance<TokenStorage> tokenStorage) {
        this.consulClientFactory = consulClientFactory;
        this.tokenStorage = tokenStorage.get();
        this.readTimeoutMillis = Duration.ofMinutes(10).toMillis();
    }

    @Override
    public void awaitChanges(String path, long index, Duration wait, PollResultHandler handler) {
        BlockingQueryOptions bq = new BlockingQueryOptions()
                .setIndex(index)
                .setWait(format(wait));

        String token = tokenStorage.get();
        io.vertx.ext.consul.ConsulClient consulClient = consulClientFactory.create(token, readTimeoutMillis);
        log.debug("Await values from consul. path='{}', requestIndex={}, wait={}", path, index, wait);
        consulClient.getValuesWithOptions(path, bq, ar -> {
            try {
                handle(path, handler, ar);
            } finally {
                consulClient.close();
            }
        });
    }

    void handle(String path,
                PollResultHandler handler,
                AsyncResult<KeyValueList> ar) {
        if (ar.succeeded()) {
            log.debug("Consul long-poll succeeded: path='{}' -> proceed snapshot", path);
            ConsulPrefixSnapshot snapshot = new ConsulPrefixSnapshot(ar.result());
            handler.onSuccess(snapshot);
        } else {
            log.warn("Consul long-poll failed: path='{}'", path, ar.cause());
            handler.onError(ar.cause());
        }
    }

    static String format(Duration wait) {
        long seconds = Math.max(1, (long) Math.ceil(wait.toMillis() / 1000.0));
        return seconds + "s";
    }
}
