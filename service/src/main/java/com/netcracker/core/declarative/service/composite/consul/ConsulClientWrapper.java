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
    private final io.vertx.ext.consul.ConsulClient delegate;

    @Inject
    public ConsulClientWrapper(ConsulClientFactory consulClientFactory,
                               Instance<TokenStorage> tokenStorage) {
        this.delegate = consulClientFactory.create(tokenStorage.get().get());
    }

    @Override
    public void awaitChanges(String path, long index, Duration wait, PollResultHandler handler) {
        log.info("VLLA awaitChanges for path '{}'", path);
        BlockingQueryOptions bq = new BlockingQueryOptions()
                .setIndex(index)
                .setWait(format(wait));

        delegate.getValuesWithOptions(path, bq, ar -> handle(handler, ar));
    }

    void handle(PollResultHandler handler, AsyncResult<KeyValueList> ar) {
        log.debug("VLLA handle ar {}", ar);
        if (ar.succeeded()) {
            ConsulPrefixSnapshot snapshot = new ConsulPrefixSnapshot(ar.result());
            handler.onSuccess(snapshot);
        }
        else {
            handler.onError(ar.cause());
        }
    }

    static String format(Duration wait) {
        long seconds = Math.max(1, (long) Math.ceil(wait.toMillis() / 1000.0));
        return seconds + "s";
    }
}
