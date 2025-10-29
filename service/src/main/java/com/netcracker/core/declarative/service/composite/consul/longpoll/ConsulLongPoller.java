package com.netcracker.core.declarative.service.composite.consul.longpoll;

import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import com.netcracker.core.declarative.service.composite.consul.ConsulClient;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Slf4j
public final class ConsulLongPoller implements AutoCloseable {
    private static final Duration DELAY_ON_ERROR = Duration.ofSeconds(10);

    private final String path;
    private final ConsulClient consulClient;
    private final LongPollConfig pollConfig;
    private final Consumer<ConsulPrefixSnapshot> onSnapshot;
    private final PollScheduler pollScheduler;

    private final PollSession pollSession = new PollSession();
    private final AtomicBoolean started = new AtomicBoolean(false);

    @Builder
    private ConsulLongPoller(String path,
                             ConsulClient consulClient,
                             LongPollConfig pollConfig,
                             Consumer<ConsulPrefixSnapshot> onSnapshot) {
        this.path = Objects.requireNonNull(path, "path");
        this.consulClient = Objects.requireNonNull(consulClient, "consulClient");
        this.pollConfig = (pollConfig != null ? pollConfig : LongPollConfig.builder().build());
        this.onSnapshot = Objects.requireNonNull(onSnapshot, "onSnapshot");
        this.pollScheduler = new PollScheduler(path);
    }

    public void start() {
        if (started.getAndSet(true)) {
            return;
        }
        scheduleNextPoll(Duration.ZERO);
        log.info("KV poller started: path='{}', cfg={}", path, pollConfig);
    }

    public void stop() {
        if (pollScheduler.isClosed()) {
            return;
        }
        pollScheduler.stop();
        log.info("KV poller stopped: path='{}'", path);
    }

    @Override
    public void close() {
        stop();
    }

    private void scheduleNextPoll(Duration delay) {
        pollScheduler.schedule(delay, this::pollOnce);
    }

    private void pollOnce() {
        if (pollScheduler.isClosed()) {
            return;
        }

        final long currentIndex = pollSession.currentIndex();
        final Duration wait = pollConfig.getWait();

        consulClient.awaitChanges(path, currentIndex, wait, new PollResultHandlerImpl(currentIndex));
    }


    private final class PollResultHandlerImpl implements PollResultHandler {
        private final long currentIndex;

        private PollResultHandlerImpl(long currentIndex) {
            this.currentIndex = currentIndex;
        }

        @Override
        public void onSuccess(ConsulPrefixSnapshot snapshot) {
            if (pollScheduler.isClosed()) {
                return;
            }

            scheduleNextPoll(Duration.ZERO);

            final long newIndex = snapshot.getIndex();
            if (pollSession.shouldEmit(currentIndex, newIndex, pollConfig.isFireOnFirstSuccess())) {
                onSnapshot.accept(snapshot);
            }
        }

        @Override
        public void onError(Throwable err) {
            log.warn("KV poller error: path='{}', retry in {}, cause='{}'", path, DELAY_ON_ERROR, err.toString());
            scheduleNextPoll(DELAY_ON_ERROR);
        }
    }
}
