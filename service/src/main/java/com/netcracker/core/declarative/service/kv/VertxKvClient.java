package com.netcracker.core.declarative.service.kv;

import io.vertx.ext.consul.BlockingQueryOptions;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueList;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;

/** Адаптер Vert.x ConsulClient → KvClient. */
public final class VertxKvClient implements KvClient {
    private final ConsulClient delegate;

    public VertxKvClient(ConsulClient delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public void awaitChanges(String path, long index, Duration wait, BiConsumer<KeyValueList, Throwable> handler) {
        final BlockingQueryOptions bq = new BlockingQueryOptions()
                .setIndex(index)
                .setWait(format(wait));
        delegate.getValuesWithOptions(path, bq, ar -> {
            if (ar.succeeded()) handler.accept(ar.result(), null);
            else handler.accept(null, ar.cause());
        });
    }

    String format(Duration wait) {
        long seconds = Math.max(1, (long) Math.ceil(wait.toMillis() / 1000.0));
        return seconds + "s";
    }
}
