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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Инфраструктурный long-pollер KV (Single Responsibility):
 *  - управляет только циклом запросов, индексом и ретраями;
 *  - НЕ знает про домен и обработку состояний.
 * Open/Closed: расширяется стратегиями и конфигом без модификации класса.
 */
@Slf4j
public final class KvLongPoller implements AutoCloseable {

    public enum State {NEW, RUNNING, BACKING_OFF, STOPPED}

    @Getter
    private final String path;
    private final KvWatcher client;
    private final ScheduledExecutorService scheduler;
    private final KvPollConfig cfg;
    private final BackoffStrategy backoff;

    private final Consumer<KeyValueList> onSnapshot;
    private final Consumer<State> onState;

    private final AtomicLong index = new AtomicLong(0);
    private final AtomicBoolean firstFired = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private volatile State state = State.NEW;
    private volatile ScheduledFuture<?> future;

    @Builder
    private KvLongPoller(String path,
                         KvWatcher client,
                         ScheduledExecutorService scheduler,
                         KvPollConfig cfg,
                         BackoffStrategy backoff,
                         Consumer<KeyValueList> onSnapshot,
                         Consumer<State> onState) {
        this.path = Objects.requireNonNull(path, "path");
        this.client = Objects.requireNonNull(client, "client");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.cfg = (cfg != null ? cfg : KvPollConfig.builder().build());
        this.backoff = (backoff != null ? backoff : new ExponentialJitterBackoff());
        this.onSnapshot = Objects.requireNonNull(onSnapshot, "onSnapshot");
        this.onState = (onState != null ? onState : s -> {
        });
        validate();
    }

    private void validate() {
        if (cfg.getBackoffMin().isNegative() || cfg.getBackoffMax().isNegative())
            throw new IllegalArgumentException("Backoff must be non-negative");
        if (cfg.getBackoffMin().compareTo(cfg.getBackoffMax()) > 0)
            throw new IllegalArgumentException("backoffMin must be <= backoffMax");
        if (!cfg.getWait().isPositive())
            throw new IllegalArgumentException("wait must be > 0");
    }

    public void start() {
        if (!started.compareAndSet(false, true)) return;
        transition(State.RUNNING);
        schedule(cfg.getInitialDelay(), Duration.ZERO);
        log.info("KV poller started: path='{}', cfg={}", path, cfg);
    }

    public void stop() {
        transition(State.STOPPED);
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
        if (isSchedulerClosed() || state == State.STOPPED) {
            return;
        }
        ScheduledFuture<?> f = future;
        if (f != null) {
            f.cancel(true);
        }
        future = scheduler.schedule(() -> pollOnce(nextBackoff), delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void pollOnce(Duration backoffDelay) {
        if (isSchedulerClosed() || state == State.STOPPED) {
            return;
        }

        final long prev = index.get();
        final Duration wait = cfg.getWait();

        client.awaitChanges(path, prev, wait, new KvHandler() {
            @Override
            public void onSuccess(KeyValueList list) {
                if (isSchedulerClosed() || state == State.STOPPED) {
                    return;
                }

                final long newIdx = (list != null ? list.getIndex() : 0);
                transition(State.RUNNING);
                schedule(Duration.ZERO, Duration.ZERO); // следующий long-poll — сразу

                final boolean changed = newIdx > prev;
                final boolean first = cfg.isFireOnFirstSuccess() && firstFired.compareAndSet(false, true);
                if (changed || first) {
                    if (changed) {
                        index.set(newIdx);
                    }
                    onSnapshot.accept(list);
                }
            }

            @Override
            public void onError(Throwable err) {
                final Duration next = backoff.next(backoffDelay, cfg.getBackoffMin(), cfg.getBackoffMax());
                transition(State.BACKING_OFF);
                log.warn("KV poller error: path='{}', retry in {}, cause='{}'",
                        path, next, err.toString());
                schedule(next, next);
            }
        });
    }

    private boolean isSchedulerClosed() {
        return scheduler.isShutdown() || scheduler.isTerminated();
    }

    private void transition(State s) {
        if (this.state == s) {
            return;
        }
        this.state = s;
        try {
            onState.accept(s);
        } catch (Exception ignore) {
            /* no-op */
        }
    }
}
