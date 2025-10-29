package com.netcracker.core.declarative.service.kv;

import io.vertx.ext.consul.KeyValueList;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Slf4j
public final class KvLongPoller implements AutoCloseable {
    @Getter
    private final String path;
    private final KvWatcher watcher;
    private final ScheduledExecutorService scheduler;
    private final KvPollConfig pollConfig;
    private final BackoffStrategy backoff;

    private final Consumer<KeyValueList> onSnapshot;

    private final AtomicLong index = new AtomicLong(0);
    private final AtomicBoolean firstFired = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private volatile ScheduledFuture<?> future;

    @Builder
    private KvLongPoller(String path,
                         KvWatcher watcher,
                         ScheduledExecutorService scheduler,
                         KvPollConfig pollConfig,
                         BackoffStrategy backoff,
                         Consumer<KeyValueList> onSnapshot) {
        this.path = Objects.requireNonNull(path, "path");
        this.watcher = Objects.requireNonNull(watcher, "watcher");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.pollConfig = (pollConfig != null ? pollConfig : KvPollConfig.builder().build());
        this.backoff = (backoff != null ? backoff : new ExponentialJitterBackoff());
        this.onSnapshot = Objects.requireNonNull(onSnapshot, "onSnapshot");
        validate();
    }

    private void validate() {
        if (pollConfig.getBackoffMin().isNegative() || pollConfig.getBackoffMax().isNegative())
            throw new IllegalArgumentException("Backoff must be non-negative");
        if (pollConfig.getBackoffMin().compareTo(pollConfig.getBackoffMax()) > 0)
            throw new IllegalArgumentException("backoffMin must be <= backoffMax");
        if (!pollConfig.getWait().isPositive())
            throw new IllegalArgumentException("wait must be > 0");
    }

    public void start() {
        if (!started.compareAndSet(false, true)) return;
        schedule(pollConfig.getInitialDelay(), Duration.ZERO);
        log.info("KV poller started: path='{}', cfg={}", path, pollConfig);
    }

    public void stop() {
        ScheduledFuture<?> f = future;
        if (f != null) f.cancel(true);
        future = null;
        log.info("KV poller stopped: path='{}'", path);
    }

    @Override
    public void close() {
        stop();
    }

    private void schedule(Duration delay, Duration nextBackoff) {
        if (isSchedulerClosed()) {
            return;
        }
        ScheduledFuture<?> f = future;
        if (f != null) {
            f.cancel(true);
        }
        future = scheduler.schedule(() -> pollOnce(nextBackoff), delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void pollOnce(Duration backoffDelay) {
        if (isSchedulerClosed()) {
            return;
        }

        final long prev = index.get();
        final Duration wait = pollConfig.getWait();

        watcher.awaitChanges(path, prev, wait, new KvHandler() {
            @Override
            public void onSuccess(KeyValueList list) {
                if (isSchedulerClosed()) {
                    return;
                }

                final long newIdx = (list != null ? list.getIndex() : 0);
                schedule(Duration.ZERO, Duration.ZERO);

                final boolean changed = newIdx > prev;
                final boolean first = pollConfig.isFireOnFirstSuccess() && firstFired.compareAndSet(false, true);
                if (changed || first) {
                    if (changed) {
                        index.set(newIdx);
                    }
                    onSnapshot.accept(list);
                }
            }

            @Override
            public void onError(Throwable err) {
                final Duration next = backoff.next(backoffDelay, pollConfig.getBackoffMin(), pollConfig.getBackoffMax());
                log.warn("KV poller error: path='{}', retry in {}, cause='{}'",
                        path, next, err.toString());
                schedule(next, next);
            }
        });
    }

    private boolean isSchedulerClosed() {
        return scheduler.isShutdown() || scheduler.isTerminated();
    }
}
