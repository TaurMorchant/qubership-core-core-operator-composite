package com.netcracker.core.declarative.service.kv;

import io.vertx.core.AsyncResult;
import io.vertx.ext.consul.BlockingQueryOptions;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueList;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Objects;

@Slf4j
public final class ConsulKvWatcher implements KvWatcher {
    private final ConsulClient consulClient;

    public ConsulKvWatcher(ConsulClient consulClient) {
        this.consulClient = Objects.requireNonNull(consulClient);
    }

    @Override
    public void awaitChanges(String path, long index, Duration wait, KvHandler handler) {
        log.info("VLLA awaitChanges for path '{}'", path);
        BlockingQueryOptions bq = new BlockingQueryOptions()
                .setIndex(index)
                .setWait(format(wait));

        consulClient.getValuesWithOptions(path, bq, ar -> handle(handler, ar));
    }

    void handle(KvHandler handler, AsyncResult<KeyValueList> ar) {
        log.debug("VLLA handle ar {}", ar);
        if (ar.succeeded()) {
            handler.onSuccess(ar.result());
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
