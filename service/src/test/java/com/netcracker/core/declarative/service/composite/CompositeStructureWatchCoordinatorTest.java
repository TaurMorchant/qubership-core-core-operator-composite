package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.client.k8s.ConfigMapClient;
import com.netcracker.core.declarative.service.composite.consul.ConsulClient;
import com.netcracker.core.declarative.service.composite.consul.longpoll.ConsulLongPoller;
import com.netcracker.core.declarative.service.composite.consul.longpoll.ConsulLongPoller.ConsulLongPollerBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CompositeStructureWatchCoordinatorTest {

    @Test
    void startShouldStartPollingWhenManagedByCoreOperator() {
        ConsulClient consulClient = mock(ConsulClient.class);
        CompositeStructureSnapshotHandler handler = mock(CompositeStructureSnapshotHandler.class);
        ConfigMapClient configMapClient = mock(ConfigMapClient.class);
        when(configMapClient.isManagedByCoreOperator("composite-structure", "ns")).thenReturn(true);

        ConsulLongPollerBuilder builder = mock(ConsulLongPollerBuilder.class, RETURNS_SELF);
        ConsulLongPoller refPoller = mock(ConsulLongPoller.class);
        when(builder.build()).thenReturn(refPoller);

        try (MockedStatic<ConsulLongPoller> mockedStatic = Mockito.mockStatic(ConsulLongPoller.class)) {
            mockedStatic.when(ConsulLongPoller::builder).thenReturn(builder);

            CompositeStructureWatchCoordinator manager = new CompositeStructureWatchCoordinator("ns", consulClient, handler, configMapClient);
            manager.start();

            verify(refPoller).start();
        }
    }

    @Test
    void startShouldSkipPollingWhenManagedByTopologyOperator() {
        ConsulClient consulClient = mock(ConsulClient.class);
        CompositeStructureSnapshotHandler handler = mock(CompositeStructureSnapshotHandler.class);
        ConfigMapClient configMapClient = mock(ConfigMapClient.class);
        when(configMapClient.isManagedByCoreOperator("composite-structure", "ns")).thenReturn(false);

        try (MockedStatic<ConsulLongPoller> mockedStatic = Mockito.mockStatic(ConsulLongPoller.class)) {
            CompositeStructureWatchCoordinator manager = new CompositeStructureWatchCoordinator("ns", consulClient, handler, configMapClient);
            manager.start();

            mockedStatic.verifyNoInteractions();
        }
    }
}
