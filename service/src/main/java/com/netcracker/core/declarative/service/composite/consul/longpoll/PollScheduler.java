package com.netcracker.core.declarative.service.composite.consul.longpoll;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public final class PollScheduler {
    private static final String THREAD_NAME_TEMPLATE = "kv-poller-%s-%d";
    private static final AtomicLong THREAD_SEQ = new AtomicLong();

    private final ScheduledExecutorService executor;
    private final Object lock = new Object();
    private ScheduledFuture<?> future;

    public PollScheduler(String threadName) {
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, resolveThreadName(threadName));
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((th, ex) ->
                    log.error("Uncaught exception in '{}'", th.getName(), ex));
            return t;
        });
    }

    public void schedule(Duration delay, Runnable task) {
        synchronized (lock) {
            if (isClosed()) {
                return;
            }
            ScheduledFuture<?> current = future;
            if (current != null) {
                current.cancel(true);
            }
            try {
                future = executor.schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException ex) {
                log.debug("Poll scheduler is already shut down, skipping reschedule", ex);
                future = null;
            }
        }
    }

    public void stop() {
        synchronized (lock) {
            ScheduledFuture<?> current = future;
            if (current != null) {
                current.cancel(true);
            }
            future = null;
            executor.shutdownNow();
        }
    }

    public boolean isClosed() {
        return executor.isShutdown() || executor.isTerminated();
    }

    private static String resolveThreadName(String path) {
        String sanitized = path.replaceAll("[^a-zA-Z0-9\\-_]", "-");
        long id = THREAD_SEQ.incrementAndGet();
        return THREAD_NAME_TEMPLATE.formatted(sanitized, id);
    }
}