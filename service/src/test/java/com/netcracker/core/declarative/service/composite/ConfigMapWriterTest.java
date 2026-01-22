package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.ConfigMapClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ConfigMapWriterTest {

    private static final String NAMESPACE = "test-namespace";
    private static final String CONFIG_MAP_NAME = "composite-structure";

    private ConfigMapClient configMapClient;
    private ScheduledExecutorService executor;
    private ConfigMapWriter writer;

    @BeforeEach
    void setUp() {
        configMapClient = mock(ConfigMapClient.class);
        executor = mock(ScheduledExecutorService.class);
        doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            task.run();
            return null;
        }).when(executor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        writer = new ConfigMapWriter(configMapClient, NAMESPACE, executor);
    }

    @Test
    void requestUpdateShouldCallConfigMapClient() {
        Map<String, String> payload = Map.of("key", "value");

        writer.requestUpdate(CONFIG_MAP_NAME, payload);

        verify(configMapClient).createOrUpdate(CONFIG_MAP_NAME, NAMESPACE, payload, null);
    }

    @Test
    void requestUpdateShouldRetryOnFailure() {
        Map<String, String> payload = Map.of("key", "value");
        AtomicInteger invocationCounter = new AtomicInteger();

        doAnswer(invocation -> {
            if (invocationCounter.getAndIncrement() == 0) {
                throw new RuntimeException("temporary failure");
            }
            return null;
        }).when(configMapClient).createOrUpdate(eq(CONFIG_MAP_NAME), eq(NAMESPACE), eq(payload), isNull());

        writer.requestUpdate(CONFIG_MAP_NAME, payload);

        verify(configMapClient, times(2)).createOrUpdate(CONFIG_MAP_NAME, NAMESPACE, payload, null);
    }

    @Test
    void requestUpdateShouldStopAfterMaxAttempts() {
        Map<String, String> payload = Map.of("key", "value");
        doAnswer(invocation -> {
            throw new RuntimeException("persistent failure");
        }).when(configMapClient).createOrUpdate(eq(CONFIG_MAP_NAME), eq(NAMESPACE), eq(payload), isNull());

        writer.requestUpdate(CONFIG_MAP_NAME, payload);

        verify(configMapClient, times(ConfigMapWriter.MAX_RETRY_ATTEMPTS))
                .createOrUpdate(CONFIG_MAP_NAME, NAMESPACE, payload, null);
    }
}
