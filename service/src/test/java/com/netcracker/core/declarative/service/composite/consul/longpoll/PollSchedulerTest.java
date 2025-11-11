package com.netcracker.core.declarative.service.composite.consul.longpoll;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class PollSchedulerTest {

    @Test
    void scheduleShouldExecuteProvidedTask() throws Exception {
        PollScheduler scheduler = new PollScheduler("test-path");
        try {
            CountDownLatch latch = new CountDownLatch(1);

            scheduler.schedule(Duration.ofMillis(10), latch::countDown);

            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void scheduleShouldReplaceExistingTask() throws Exception {
        PollScheduler scheduler = new PollScheduler("test-path");
        try {
            AtomicInteger counter = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(1);

            scheduler.schedule(Duration.ofSeconds(1), () -> counter.addAndGet(10));
            scheduler.schedule(Duration.ofMillis(10), () -> {
                counter.incrementAndGet();
                latch.countDown();
            });

            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(100);

            assertThat(counter.get()).isEqualTo(1);
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void scheduleShouldNotExecuteWhenStopped() throws Exception {
        PollScheduler scheduler = new PollScheduler("test-path");
        try {
            AtomicInteger counter = new AtomicInteger();

            scheduler.stop();
            scheduler.schedule(Duration.ZERO, counter::incrementAndGet);

            Thread.sleep(50);

            assertThat(counter).hasValue(0);
            assertThat(scheduler.isClosed()).isTrue();
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void scheduleShouldUseSanitizedThreadName() throws Exception {
        String path = "path with spaces/and*chars";
        PollScheduler scheduler = new PollScheduler(path);
        try {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<String> threadName = new AtomicReference<>();

            scheduler.schedule(Duration.ZERO, () -> {
                threadName.set(Thread.currentThread().getName());
                latch.countDown();
            });

            assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();

            String sanitized = path.replaceAll("[^a-zA-Z0-9\\-_]", "-");
            assertThat(threadName.get())
                    .startsWith("kv-poller-" + sanitized + "-");
        } finally {
            scheduler.stop();
        }
    }
}
