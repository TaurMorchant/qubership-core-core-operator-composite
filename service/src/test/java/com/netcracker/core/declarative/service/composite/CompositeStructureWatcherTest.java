package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.service.composite.consul.ConsulClient;
import com.netcracker.core.declarative.service.composite.consul.longpoll.ConsulLongPoller;
import com.netcracker.core.declarative.service.composite.consul.longpoll.ConsulLongPollerFactory;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CompositeStructureWatcherTest {

    private static final String NAMESPACE = "ns";
    private static final String STRUCTURE_REF_KEY = "config/ns/application/composite/structureRef";

    private ConsulClient consulClient;
    private CompositeStructureSnapshotHandler snapshotHandler;
    private ConsulLongPollerFactory pollerFactory;
    private ConsulLongPoller refPoller;
    private ConsulLongPoller structurePoller;
    private Consumer<ConsulPrefixSnapshot> capturedRefCallback;

    private CompositeStructureWatcher watcher;

    @BeforeEach
    void setUp() {
        consulClient = mock(ConsulClient.class);
        snapshotHandler = mock(CompositeStructureSnapshotHandler.class);
        pollerFactory = mock(ConsulLongPollerFactory.class);
        refPoller = mock(ConsulLongPoller.class);
        structurePoller = mock(ConsulLongPoller.class);

        when(pollerFactory.create(eq(STRUCTURE_REF_KEY), eq(consulClient), any(), any()))
                .thenAnswer(invocation -> {
                    capturedRefCallback = invocation.getArgument(3);
                    return refPoller;
                });

        watcher = new CompositeStructureWatcher(NAMESPACE, consulClient, snapshotHandler, pollerFactory);
    }

    @Test
    void startShouldCreateAndStartRefPoller() {
        watcher.start();

        verify(pollerFactory).create(eq(STRUCTURE_REF_KEY), eq(consulClient), any(), any());
        verify(refPoller).start();
    }

    @Test
    void startShouldBeIdempotent() {
        watcher.start();
        watcher.start();

        verify(refPoller).start();
    }

    @Test
    void stopShouldCloseRefPoller() {
        watcher.start();
        watcher.stop();

        verify(refPoller).close();
    }

    @Test
    void newPrefixShouldStartStructurePoller() {
        when(pollerFactory.create(eq("prefix/one"), eq(consulClient), any(), any()))
                .thenReturn(structurePoller);

        watcher.start();
        simulateRefSnapshot("prefix/one");

        verify(pollerFactory).create(eq("prefix/one"), eq(consulClient), any(), any());
        verify(structurePoller).start();
    }

    @Test
    void prefixChangeShouldSwitchPoller() {
        ConsulLongPoller newStructurePoller = mock(ConsulLongPoller.class);
        when(pollerFactory.create(eq("prefix/one"), eq(consulClient), any(), any()))
                .thenReturn(structurePoller);
        when(pollerFactory.create(eq("prefix/two"), eq(consulClient), any(), any()))
                .thenReturn(newStructurePoller);

        watcher.start();
        simulateRefSnapshot("prefix/one");
        simulateRefSnapshot("prefix/two");

        verify(structurePoller).close();
        verify(newStructurePoller).start();
    }

    @Test
    void samePrefixShouldNotRestartPoller() {
        when(pollerFactory.create(eq("prefix/one"), eq(consulClient), any(), any()))
                .thenReturn(structurePoller);

        watcher.start();
        simulateRefSnapshot("prefix/one");
        simulateRefSnapshot("prefix/one");

        verify(structurePoller).start();
        verify(structurePoller, never()).close();
    }

    @Test
    void blankPrefixShouldStopPoller() {
        when(pollerFactory.create(eq("prefix/one"), eq(consulClient), any(), any()))
                .thenReturn(structurePoller);

        watcher.start();
        simulateRefSnapshot("prefix/one");
        simulateRefSnapshot("   ");

        verify(structurePoller).close();
    }

    @Test
    void stopShouldCloseAllPollers() {
        when(pollerFactory.create(eq("prefix/one"), eq(consulClient), any(), any()))
                .thenReturn(structurePoller);

        watcher.start();
        simulateRefSnapshot("prefix/one");
        watcher.stop();

        verify(refPoller).close();
        verify(structurePoller).close();
    }

    @Test
    void snapshotAfterStopShouldBeIgnored() {
        when(pollerFactory.create(eq("prefix/one"), eq(consulClient), any(), any()))
                .thenReturn(structurePoller);

        watcher.start();
        watcher.stop();
        simulateRefSnapshot("prefix/one");

        verify(structurePoller, never()).start();
    }

    private void simulateRefSnapshot(String prefix) {
        ConsulPrefixSnapshot snapshot = mock(ConsulPrefixSnapshot.class);
        when(snapshot.getValue(STRUCTURE_REF_KEY)).thenReturn(prefix);
        capturedRefCallback.accept(snapshot);
    }
}
