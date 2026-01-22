package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.ConfigMapClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CompositeStructureWatchCoordinatorTest {

    private static final String NAMESPACE = "test-ns";

    private ConfigMapClient configMapClient;
    private CompositeStructureWatcher watcher;
    private ScheduledExecutorService executor;
    private CompositeStructureWatchCoordinator coordinator;

    @BeforeEach
    void setUp() {
        configMapClient = mock(ConfigMapClient.class);
        watcher = mock(CompositeStructureWatcher.class);
        executor = mock(ScheduledExecutorService.class);
        doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            task.run();
            return null;
        }).when(executor).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

        coordinator = new CompositeStructureWatchCoordinator(NAMESPACE, configMapClient, watcher, executor);
    }

    @Test
    void startShouldStartWatcherWhenManagedByCoreOperator() {
        when(configMapClient.isManagedByCoreOperator("composite-structure", NAMESPACE)).thenReturn(true);

        coordinator.start();

        verify(configMapClient).isManagedByCoreOperator("composite-structure", NAMESPACE);
        verify(watcher).start();
    }

    @Test
    void startShouldNotStartWatcherWhenNotManagedByCoreOperator() {
        when(configMapClient.isManagedByCoreOperator("composite-structure", NAMESPACE)).thenReturn(false);

        coordinator.start();

        verify(configMapClient).isManagedByCoreOperator("composite-structure", NAMESPACE);
        verify(watcher, never()).start();
    }

    @Test
    void startShouldStopWatcherWhenOwnershipChanges() {
        when(configMapClient.isManagedByCoreOperator("composite-structure", NAMESPACE))
                .thenReturn(true)
                .thenReturn(false);

        doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            task.run();
            task.run();
            return null;
        }).when(executor).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));

        coordinator.start();

        verify(watcher).start();
        verify(watcher).stop();
    }

    @Test
    void startShouldNotFailOnConfigMapClientException() {
        when(configMapClient.isManagedByCoreOperator("composite-structure", NAMESPACE))
                .thenThrow(new RuntimeException("API error"));

        coordinator.start();

        verify(watcher, never()).start();
        verify(watcher, never()).stop();
    }

    @Test
    void stopShouldStopWatcherAndShutdownExecutor() {
        when(configMapClient.isManagedByCoreOperator("composite-structure", NAMESPACE)).thenReturn(true);

        coordinator.start();
        coordinator.stop();

        verify(watcher).stop();
        verify(executor).shutdownNow();
    }
}
