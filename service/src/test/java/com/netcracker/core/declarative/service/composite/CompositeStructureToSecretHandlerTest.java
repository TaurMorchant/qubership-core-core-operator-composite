package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.SecretClient;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class CompositeStructureToSecretHandlerTest {

    private static final String NAMESPACE = "test-namespace";
    private static final String SECRET_NAME = "composite-structure";

    private SecretClient secretClient;
    private CompositeStructureToSecretHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        secretClient = mock(SecretClient.class);
        handler = new CompositeStructureToSecretHandler(secretClient, NAMESPACE);
        replaceExecutor(handler, new ImmediateScheduledThreadPoolExecutor());
    }

    @AfterEach
    void tearDown() {
        handler.shutdownExecutor();
    }

    @Test
    void handleShouldSerializeSnapshotAndUpdateSecret() {
        ConsulPrefixSnapshot snapshot = snapshot(Map.of(
                "composite/sample/structure/ns-a/compositeRole", "baseline",
                "composite/sample/structure/ns-b/compositeRole", "satellite"
        ));

        handler.handle(snapshot);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> dataCaptor = ArgumentCaptor.forClass(Map.class);
        verify(secretClient).createOrUpdate(eq(SECRET_NAME), eq(NAMESPACE), dataCaptor.capture(), isNull());

        Map<String, String> data = dataCaptor.getValue();
        assertEquals(1, data.size());
        assertEquals("{\"baseline\":{\"origin\":\"ns-a\"},\"satellites\":[{\"origin\":\"ns-b\"}]}",
                data.get("data"));
    }

    @Test
    void updateSecretWithRetryRetriesAfterFailure() {
        Map<String, String> payload = Map.of("compositeStructure", "{}");
        AtomicInteger invocationCounter = new AtomicInteger();

        doAnswer(invocation -> {
            if (invocationCounter.getAndIncrement() == 0) {
                throw new RuntimeException("boom");
            }
            return null;
        }).when(secretClient).createOrUpdate(eq(SECRET_NAME), eq(NAMESPACE), eq(payload), isNull());

        handler.updateSecretWithRetry(payload, 1, Duration.ofMillis(1));

        verify(secretClient, times(2)).createOrUpdate(eq(SECRET_NAME), eq(NAMESPACE), eq(payload), isNull());
    }

    @Test
    void updateSecretWithRetryStopsAfterMaxAttempts() throws Exception {
        Map<String, String> payload = Map.of("compositeStructure", "{}");
        doAnswer(invocation -> {
            throw new RuntimeException("boom");
        }).when(secretClient).createOrUpdate(eq(SECRET_NAME), eq(NAMESPACE), eq(payload), isNull());

        int maxAttempts = getMaxRetryAttempts();
        handler.updateSecretWithRetry(payload, maxAttempts, Duration.ZERO);

        verify(secretClient, times(1)).createOrUpdate(eq(SECRET_NAME), eq(NAMESPACE), eq(payload), isNull());
    }

    private static void replaceExecutor(CompositeStructureToSecretHandler target, ScheduledThreadPoolExecutor executor) throws Exception {
        Field field = CompositeStructureToSecretHandler.class.getDeclaredField("k8sWritesExecutorService");
        field.setAccessible(true);
        field.set(target, executor);
    }

    private static ConsulPrefixSnapshot snapshot(Map<String, String> keyValues) {
        KeyValueList keyValueList = new KeyValueList();
        keyValueList.setList(keyValues.entrySet().stream()
                .map(entry -> new KeyValue().setKey(entry.getKey()).setValue(entry.getValue()))
                .toList());
        return new ConsulPrefixSnapshot(keyValueList);
    }

    private static int getMaxRetryAttempts() throws Exception {
        Field field = CompositeStructureToSecretHandler.class.getDeclaredField("MAX_RETRY_ATTEMPTS");
        field.setAccessible(true);
        return field.getInt(null);
    }

    private static class ImmediateScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
        ImmediateScheduledThreadPoolExecutor() {
            super(1);
        }

        @Override
        public void execute(Runnable command) {
            command.run();
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
