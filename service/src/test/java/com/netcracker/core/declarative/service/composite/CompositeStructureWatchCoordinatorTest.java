package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.ConfigMapClient;
import com.netcracker.core.declarative.service.composite.consul.ConsulClient;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class CompositeStructureWatchCoordinatorTest {

    @Test
    void startShouldStartPollingWhenManagedByCoreOperator() throws Exception {
        ConsulClient consulClient = mock(ConsulClient.class);
        CompositeStructureSnapshotHandler handler = mock(CompositeStructureSnapshotHandler.class);
        ConfigMapClient configMapClient = mock(ConfigMapClient.class);
        when(configMapClient.isManagedByCoreOperator("composite-structure", "ns")).thenReturn(true);

        CompositeStructureWatcher watcher = mock(CompositeStructureWatcher.class);
        CountDownLatch watcherStarted = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            watcherStarted.countDown();
            return null;
        }).when(watcher).start();

        CompositeStructureWatchCoordinator manager = new CompositeStructureWatchCoordinator("ns", consulClient, handler, configMapClient);
        setWatcher(manager, watcher);
        manager.start();

        assertTrue(watcherStarted.await(1, TimeUnit.SECONDS));
        verify(configMapClient).isManagedByCoreOperator("composite-structure", "ns");
        verify(watcher).start();
        manager.stop();
    }

    @Test
    void startShouldSkipPollingWhenManagedByTopologyOperator() throws Exception {
        ConsulClient consulClient = mock(ConsulClient.class);
        CompositeStructureSnapshotHandler handler = mock(CompositeStructureSnapshotHandler.class);
        ConfigMapClient configMapClient = mock(ConfigMapClient.class);
        CompositeStructureWatcher watcher = mock(CompositeStructureWatcher.class);
        CountDownLatch configChecked = new CountDownLatch(1);
        when(configMapClient.isManagedByCoreOperator("composite-structure", "ns"))
                .thenAnswer(invocation -> {
                    configChecked.countDown();
                    return false;
                });

        CompositeStructureWatchCoordinator manager = new CompositeStructureWatchCoordinator("ns", consulClient, handler, configMapClient);
        setWatcher(manager, watcher);
        manager.start();

        assertTrue(configChecked.await(1, TimeUnit.SECONDS));
        verify(configMapClient).isManagedByCoreOperator("composite-structure", "ns");
        verifyNoInteractions(watcher);
        manager.stop();
    }

    private static void setWatcher(CompositeStructureWatchCoordinator manager, CompositeStructureWatcher watcher)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = CompositeStructureWatchCoordinator.class.getDeclaredField("compositeStructureWatcher");
        field.setAccessible(true);
        field.set(manager, watcher);
    }
}
