package com.netcracker.core.declarative.service.composite.consul.longpoll;

import com.netcracker.core.declarative.service.composite.consul.ConsulClient;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@SuppressWarnings("unchecked")
class ConsulLongPollerPollResultHandlerImplTest {

    @Test
    void onSuccessShouldRescheduleAndEmitWhenSessionAllows() throws Exception {
        Consumer<ConsulPrefixSnapshot> onSnapshot = mock(Consumer.class);
        ConsulClient consulClient = mock(ConsulClient.class);
        ConsulLongPoller poller = createPoller(consulClient, onSnapshot);
        PollScheduler scheduler = mock(PollScheduler.class);
        doReturn(false).when(scheduler).isClosed();
        injectField(poller, "pollScheduler", scheduler);

        PollSession pollSession = mock(PollSession.class);
        doReturn(7L).when(pollSession).currentIndex();
        doReturn(true).when(pollSession).shouldEmit(anyLong(), anyLong(), anyBoolean());
        injectField(poller, "pollSession", pollSession);

        PollResultHandler handler = pollOnceAndCaptureHandler(poller, consulClient, 7L);

        ConsulPrefixSnapshot snapshot = mock(ConsulPrefixSnapshot.class);
        doReturn(10L).when(snapshot).getIndex();

        handler.onSuccess(snapshot);

        verify(scheduler, times(2)).isClosed();
        ArgumentCaptor<Duration> durationCaptor = ArgumentCaptor.forClass(Duration.class);
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).schedule(durationCaptor.capture(), runnableCaptor.capture());
        assertThat(durationCaptor.getValue()).isEqualTo(Duration.ZERO);
        assertThat(runnableCaptor.getValue()).isNotNull();
        verify(pollSession).shouldEmit(7L, 10L, true);
        verify(onSnapshot).accept(snapshot);
    }

    @Test
    void onSuccessShouldRescheduleButNotEmitWhenSessionBlocks() throws Exception {
        Consumer<ConsulPrefixSnapshot> onSnapshot = mock(Consumer.class);
        ConsulClient consulClient = mock(ConsulClient.class);
        ConsulLongPoller poller = createPoller(consulClient, onSnapshot);
        PollScheduler scheduler = mock(PollScheduler.class);
        doReturn(false).when(scheduler).isClosed();
        injectField(poller, "pollScheduler", scheduler);

        PollSession pollSession = mock(PollSession.class);
        doReturn(11L).when(pollSession).currentIndex();
        doReturn(false).when(pollSession).shouldEmit(anyLong(), anyLong(), anyBoolean());
        injectField(poller, "pollSession", pollSession);

        PollResultHandler handler = pollOnceAndCaptureHandler(poller, consulClient, 11L);

        ConsulPrefixSnapshot snapshot = mock(ConsulPrefixSnapshot.class);
        doReturn(12L).when(snapshot).getIndex();

        handler.onSuccess(snapshot);

        verify(scheduler, times(2)).isClosed();
        ArgumentCaptor<Duration> durationCaptor = ArgumentCaptor.forClass(Duration.class);
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).schedule(durationCaptor.capture(), runnableCaptor.capture());
        assertThat(durationCaptor.getValue()).isEqualTo(Duration.ZERO);
        assertThat(runnableCaptor.getValue()).isNotNull();
        verify(pollSession).shouldEmit(11L, 12L, true);
        verify(onSnapshot, never()).accept(any());
    }

    @Test
    void onSuccessShouldReturnImmediatelyWhenSchedulerClosed() throws Exception {
        Consumer<ConsulPrefixSnapshot> onSnapshot = mock(Consumer.class);
        ConsulClient consulClient = mock(ConsulClient.class);
        ConsulLongPoller poller = createPoller(consulClient, onSnapshot);
        PollScheduler scheduler = mock(PollScheduler.class);
        doReturn(false).when(scheduler).isClosed();
        injectField(poller, "pollScheduler", scheduler);

        PollSession pollSession = mock(PollSession.class);
        injectField(poller, "pollSession", pollSession);

        PollResultHandler handler = pollOnceAndCaptureHandler(poller, consulClient, 0L);

        reset(scheduler, pollSession, onSnapshot);
        doReturn(true).when(scheduler).isClosed();

        handler.onSuccess(mock(ConsulPrefixSnapshot.class));

        verify(scheduler).isClosed();
        verify(scheduler, never()).schedule(any(Duration.class), any(Runnable.class));
        verifyNoInteractions(pollSession);
        verifyNoInteractions(onSnapshot);
    }

    private static ConsulLongPoller createPoller(ConsulClient consulClient, Consumer<ConsulPrefixSnapshot> onSnapshot) {
        return ConsulLongPoller.builder()
                .path("path")
                .consulClient(consulClient)
                .onSnapshot(onSnapshot)
                .build();
    }

    private static PollResultHandler pollOnceAndCaptureHandler(ConsulLongPoller poller,
                                                               ConsulClient consulClient,
                                                               long expectedIndex) throws Exception {
        Method pollOnce = ConsulLongPoller.class.getDeclaredMethod("pollOnce");
        pollOnce.setAccessible(true);
        pollOnce.invoke(poller);

        ArgumentCaptor<PollResultHandler> handlerCaptor = ArgumentCaptor.forClass(PollResultHandler.class);
        verify(consulClient).awaitChanges(eq("path"), eq(expectedIndex), eq(Duration.ofMinutes(9)), handlerCaptor.capture());
        return handlerCaptor.getValue();
    }

    private static void injectField(ConsulLongPoller poller, String fieldName, Object value) throws Exception {
        Field field = ConsulLongPoller.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(poller, value);
    }
}
