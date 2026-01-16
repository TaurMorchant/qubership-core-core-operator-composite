package com.netcracker.core.declarative.service.composite;

import com.netcracker.core.declarative.service.composite.consul.ConsulClient;
import com.netcracker.core.declarative.service.composite.consul.longpoll.ConsulLongPoller;
import com.netcracker.core.declarative.service.composite.consul.longpoll.ConsulLongPoller.ConsulLongPollerBuilder;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class CompositeWatcherTest {

    @Mock
    ConsulClient consulClient;

    @Mock
    CompositeStructureToConfigMapHandler snapshotHandler;

    @Mock
    ConsulLongPoller refPoller;

    @Mock
    ConsulLongPoller structurePoller;

    @Mock
    ConsulLongPoller newStructurePoller;

    @Test
    void startShouldStartCompositeStructureRefPoller() {
        ConsulLongPollerBuilder builder = mock(ConsulLongPollerBuilder.class, RETURNS_SELF);
        when(builder.build()).thenReturn(refPoller);

        try (MockedStatic<ConsulLongPoller> mockedStatic = Mockito.mockStatic(ConsulLongPoller.class)) {
            mockedStatic.when(ConsulLongPoller::builder).thenReturn(builder);

            CompositeWatcher watcher = new CompositeWatcher("ns", consulClient, snapshotHandler);
            watcher.start();

            verify(builder).path("config/ns/application/composite/structureRef");
            verify(builder).consulClient(consulClient);
            verify(builder).pollConfig(Mockito.any());
            verify(builder).onSnapshot(Mockito.any());
            verify(refPoller).start();
        }
    }

    @Test
    void newPrefixShouldSwitchCompositeStructurePoller() {
        ConsulLongPollerBuilder refBuilder = mock(ConsulLongPollerBuilder.class, RETURNS_SELF);
        ConsulLongPollerBuilder structureBuilder = mock(ConsulLongPollerBuilder.class, RETURNS_SELF);
        ConsulLongPollerBuilder secondStructureBuilder = mock(ConsulLongPollerBuilder.class, RETURNS_SELF);

        when(refBuilder.build()).thenReturn(refPoller);
        when(structureBuilder.build()).thenReturn(structurePoller);
        when(secondStructureBuilder.build()).thenReturn(newStructurePoller);
        AtomicReference<Consumer<ConsulPrefixSnapshot>> firstStructureConsumer = new AtomicReference<>();
        when(structureBuilder.onSnapshot(Mockito.any())).thenAnswer(invocation -> {
            firstStructureConsumer.set(invocation.getArgument(0));
            return structureBuilder;
        });
        AtomicReference<Consumer<ConsulPrefixSnapshot>> secondStructureConsumer = new AtomicReference<>();
        when(secondStructureBuilder.onSnapshot(Mockito.any())).thenAnswer(invocation -> {
            secondStructureConsumer.set(invocation.getArgument(0));
            return secondStructureBuilder;
        });

        try (MockedStatic<ConsulLongPoller> mockedStatic = Mockito.mockStatic(ConsulLongPoller.class)) {
            mockedStatic.when(ConsulLongPoller::builder)
                    .thenReturn(refBuilder, structureBuilder, secondStructureBuilder);

            CompositeWatcher watcher = new CompositeWatcher("ns", consulClient, snapshotHandler);
            watcher.start();

            ArgumentCaptor<Consumer<ConsulPrefixSnapshot>> refSnapshotCaptor = ArgumentCaptor.forClass(Consumer.class);
            verify(refBuilder).onSnapshot(refSnapshotCaptor.capture());

            ConsulPrefixSnapshot firstSnapshot = mock(ConsulPrefixSnapshot.class);
            when(firstSnapshot.getValue("config/ns/application/composite/structureRef")).thenReturn("prefix/one");

            refSnapshotCaptor.getValue().accept(firstSnapshot);

            verify(structureBuilder).path("prefix/one");
            verify(structurePoller).start();
            ConsulPrefixSnapshot payloadSnapshot = mock(ConsulPrefixSnapshot.class);
            firstStructureConsumer.get().accept(payloadSnapshot);
            verify(snapshotHandler).handle(payloadSnapshot);

            ConsulPrefixSnapshot secondSnapshot = mock(ConsulPrefixSnapshot.class);
            when(secondSnapshot.getValue("config/ns/application/composite/structureRef")).thenReturn("prefix/two");

            refSnapshotCaptor.getValue().accept(secondSnapshot);

            verify(structurePoller).close();
            verify(secondStructureBuilder).path("prefix/two");
            verify(newStructurePoller).start();
            ConsulPrefixSnapshot newPayloadSnapshot = mock(ConsulPrefixSnapshot.class);
            secondStructureConsumer.get().accept(newPayloadSnapshot);
            verify(snapshotHandler).handle(newPayloadSnapshot);
        }
    }

    @Test
    void samePrefixShouldNotRestartPolling() {
        ConsulLongPollerBuilder refBuilder = mock(ConsulLongPollerBuilder.class, RETURNS_SELF);
        ConsulLongPollerBuilder structureBuilder = mock(ConsulLongPollerBuilder.class, RETURNS_SELF);

        when(refBuilder.build()).thenReturn(refPoller);
        when(structureBuilder.build()).thenReturn(structurePoller);

        try (MockedStatic<ConsulLongPoller> mockedStatic = Mockito.mockStatic(ConsulLongPoller.class)) {
            mockedStatic.when(ConsulLongPoller::builder).thenReturn(refBuilder, structureBuilder);

            CompositeWatcher watcher = new CompositeWatcher("ns", consulClient, snapshotHandler);
            watcher.start();

            ArgumentCaptor<Consumer<ConsulPrefixSnapshot>> refSnapshotCaptor = ArgumentCaptor.forClass(Consumer.class);
            verify(refBuilder).onSnapshot(refSnapshotCaptor.capture());

            ConsulPrefixSnapshot snapshot = mock(ConsulPrefixSnapshot.class);
            when(snapshot.getValue("config/ns/application/composite/structureRef")).thenReturn("prefix/one");

            refSnapshotCaptor.getValue().accept(snapshot);
            refSnapshotCaptor.getValue().accept(snapshot);

            verify(structureBuilder, times(1)).build();
            verify(structurePoller, times(1)).start();
            verify(structurePoller, times(0)).close();
            mockedStatic.verify(ConsulLongPoller::builder, times(2));
        }
    }

    @Test
    void blankPrefixShouldStopCurrentPoller() {
        ConsulLongPollerBuilder refBuilder = mock(ConsulLongPollerBuilder.class, RETURNS_SELF);
        ConsulLongPollerBuilder structureBuilder = mock(ConsulLongPollerBuilder.class, RETURNS_SELF);

        when(refBuilder.build()).thenReturn(refPoller);
        when(structureBuilder.build()).thenReturn(structurePoller);

        try (MockedStatic<ConsulLongPoller> mockedStatic = Mockito.mockStatic(ConsulLongPoller.class)) {
            mockedStatic.when(ConsulLongPoller::builder).thenReturn(refBuilder, structureBuilder);

            CompositeWatcher watcher = new CompositeWatcher("ns", consulClient, snapshotHandler);
            watcher.start();

            ArgumentCaptor<Consumer<ConsulPrefixSnapshot>> refSnapshotCaptor = ArgumentCaptor.forClass(Consumer.class);
            verify(refBuilder).onSnapshot(refSnapshotCaptor.capture());

            ConsulPrefixSnapshot snapshotWithPrefix = mock(ConsulPrefixSnapshot.class);
            when(snapshotWithPrefix.getValue("config/ns/application/composite/structureRef")).thenReturn("prefix/one");
            refSnapshotCaptor.getValue().accept(snapshotWithPrefix);

            ConsulPrefixSnapshot blankSnapshot = mock(ConsulPrefixSnapshot.class);
            when(blankSnapshot.getValue("config/ns/application/composite/structureRef")).thenReturn("   ");
            refSnapshotCaptor.getValue().accept(blankSnapshot);

            verify(structurePoller).close();
            mockedStatic.verify(ConsulLongPoller::builder, times(2));
        }
    }

    @Test
    void stopShouldCloseActivePollers() {
        ConsulLongPollerBuilder refBuilder = mock(ConsulLongPollerBuilder.class, RETURNS_SELF);
        ConsulLongPollerBuilder structureBuilder = mock(ConsulLongPollerBuilder.class, RETURNS_SELF);

        when(refBuilder.build()).thenReturn(refPoller);
        when(structureBuilder.build()).thenReturn(structurePoller);

        try (MockedStatic<ConsulLongPoller> mockedStatic = Mockito.mockStatic(ConsulLongPoller.class)) {
            mockedStatic.when(ConsulLongPoller::builder).thenReturn(refBuilder, structureBuilder);

            CompositeWatcher watcher = new CompositeWatcher("ns", consulClient, snapshotHandler);
            watcher.start();

            ArgumentCaptor<Consumer<ConsulPrefixSnapshot>> refSnapshotCaptor = ArgumentCaptor.forClass(Consumer.class);
            verify(refBuilder).onSnapshot(refSnapshotCaptor.capture());

            ConsulPrefixSnapshot snapshot = mock(ConsulPrefixSnapshot.class);
            when(snapshot.getValue("config/ns/application/composite/structureRef")).thenReturn("prefix/one");
            refSnapshotCaptor.getValue().accept(snapshot);

            verify(refPoller).start();
            verify(structurePoller).start();
            watcher.stop();

            verify(refPoller).close();
            verify(structurePoller).close();
            verifyNoMoreInteractions(refPoller, structurePoller);
        }
    }
}
