package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.ConfigMapClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ConfigMapWriterTest {

    private static final String NAMESPACE = "test-namespace";
    private static final String CONFIG_MAP_NAME = "composite-structure";

    private ConfigMapClient configMapClient;
    private ConfigMapWriter updater;

    @BeforeEach
    void setUp() throws Exception {
        configMapClient = mock(ConfigMapClient.class);
        updater = new ConfigMapWriter(configMapClient, NAMESPACE);
        replaceExecutor(updater, new ImmediateScheduledThreadPoolExecutor());
    }

    @Test
    void requestUpdateRetriesAfterFailure() {
        Map<String, String> payload = Map.of("compositeStructure", "{}");
        AtomicInteger invocationCounter = new AtomicInteger();

        doAnswer(invocation -> {
            if (invocationCounter.getAndIncrement() == 0) {
                throw new KubernetesClientException("boom");
            }
            return null;
        }).when(configMapClient).createOrUpdate(eq(CONFIG_MAP_NAME), eq(NAMESPACE), eq(payload), isNull());

        updater.requestUpdate(CONFIG_MAP_NAME, payload);

        verify(configMapClient, times(2)).createOrUpdate(eq(CONFIG_MAP_NAME), eq(NAMESPACE), eq(payload), isNull());
    }

    @Test
    void requestUpdateStopsAfterMaxAttempts() throws Exception {
        Map<String, String> payload = Map.of("compositeStructure", "{}");
        doAnswer(invocation -> {
            throw new KubernetesClientException("boom");
        }).when(configMapClient).createOrUpdate(eq(CONFIG_MAP_NAME), eq(NAMESPACE), eq(payload), isNull());

        int maxAttempts = getMaxRetryAttempts();
        updater.requestUpdate(CONFIG_MAP_NAME, payload);

        verify(configMapClient, times(maxAttempts)).createOrUpdate(eq(CONFIG_MAP_NAME), eq(NAMESPACE), eq(payload), isNull());
    }

    private static void replaceExecutor(ConfigMapWriter target,
                                        ScheduledThreadPoolExecutor executor) throws Exception {
        Field field = ConfigMapWriter.class.getDeclaredField("executor");
        field.setAccessible(true);
        field.set(target, executor);
    }

    private static int getMaxRetryAttempts() throws Exception {
        Field field = ConfigMapWriter.class.getDeclaredField("MAX_RETRY_ATTEMPTS");
        field.setAccessible(true);
        return field.getInt(null);
    }

    private static class ImmediateScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
        ImmediateScheduledThreadPoolExecutor() {
            super(1);
        }

        @Override
        public @NotNull ScheduledFuture<?> schedule(Runnable command, long delay, @NotNull TimeUnit unit) {
            command.run();
            return new CompletedScheduledFuture<>();
        }
    }

    private static class CompletedScheduledFuture<V> implements ScheduledFuture<V> {

        @Override
        public long getDelay(@NotNull TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(@NotNull Delayed o) {
            return 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public V get() {
            return null;
        }

        @Override
        public V get(long timeout, @NotNull TimeUnit unit) {
            return null;
        }
    }
}
