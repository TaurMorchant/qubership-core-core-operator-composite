package com.netcracker.core.declarative.service.composite.consul;

import com.netcracker.cloud.consul.provider.common.TokenStorage;
import com.netcracker.core.declarative.service.ConsulClientFactory;
import com.netcracker.core.declarative.service.composite.consul.longpoll.PollResultHandler;
import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.ext.consul.BlockingQueryOptions;
import io.vertx.ext.consul.KeyValueList;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ConsulClientWrapperTest {

    @Test
    void awaitChangesShouldInvokeConsulAndCloseClient() {
        ConsulClientFactory consulClientFactory = mock(ConsulClientFactory.class);
        TokenStorage tokenStorage = mock(TokenStorage.class);
        doReturn("token").when(tokenStorage).get();
        @SuppressWarnings("unchecked")
        Instance<TokenStorage> tokenStorageInstance = mock(Instance.class);
        doReturn(tokenStorage).when(tokenStorageInstance).get();

        io.vertx.ext.consul.ConsulClient consulClient = mock(io.vertx.ext.consul.ConsulClient.class);
        doReturn(consulClient)
                .when(consulClientFactory)
                .create(eq("token"), anyLong());

        ConsulClientWrapper wrapper = new ConsulClientWrapper(consulClientFactory, tokenStorageInstance);
        PollResultHandler handler = mock(PollResultHandler.class);

        wrapper.awaitChanges("path", 42L, Duration.ofSeconds(30), handler);

        ArgumentCaptor<BlockingQueryOptions> optionsCaptor = ArgumentCaptor.forClass(BlockingQueryOptions.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Handler<AsyncResult<KeyValueList>>> handlerCaptor = ArgumentCaptor.forClass(Handler.class);

        verify(consulClient)
                .getValuesWithOptions(eq("path"), optionsCaptor.capture(), handlerCaptor.capture());

        BlockingQueryOptions options = optionsCaptor.getValue();
        assertThat(options.getIndex()).isEqualTo(42L);
        assertThat(options.getWait()).isEqualTo("30s");

        AsyncResult<KeyValueList> asyncResult = mock(AsyncResult.class);
        doReturn(true).when(asyncResult).succeeded();
        doReturn(mock(KeyValueList.class)).when(asyncResult).result();

        handlerCaptor.getValue().handle(asyncResult);

        verify(consulClient).close();
    }

    @Test
    void handleShouldForwardSuccessfulResult() {
        ConsulClientWrapper wrapper = createWrapper();
        PollResultHandler handler = mock(PollResultHandler.class);
        @SuppressWarnings("unchecked")
        AsyncResult<KeyValueList> asyncResult = mock(AsyncResult.class);
        doReturn(true).when(asyncResult).succeeded();
        doReturn(mock(KeyValueList.class)).when(asyncResult).result();

        wrapper.handle("path", handler, asyncResult);

        ArgumentCaptor<ConsulPrefixSnapshot> snapshotCaptor = ArgumentCaptor.forClass(ConsulPrefixSnapshot.class);
        verify(handler).onSuccess(snapshotCaptor.capture());
        assertThat(snapshotCaptor.getValue()).isNotNull();
        verify(handler, never()).onError(any());
    }

    @Test
    void handleShouldForwardFailure() {
        ConsulClientWrapper wrapper = createWrapper();
        PollResultHandler handler = mock(PollResultHandler.class);
        @SuppressWarnings("unchecked")
        AsyncResult<KeyValueList> asyncResult = mock(AsyncResult.class);
        RuntimeException failure = new RuntimeException("boom");
        doReturn(false).when(asyncResult).succeeded();
        doReturn(failure).when(asyncResult).cause();

        wrapper.handle("path", handler, asyncResult);

        verify(handler).onError(failure);
        verify(handler, never()).onSuccess(any());
    }

    @Test
    void formatShouldRoundUpToSeconds() {
        assertThat(ConsulClientWrapper.format(Duration.ofMillis(1))).isEqualTo("1s");
        assertThat(ConsulClientWrapper.format(Duration.ofMillis(1500))).isEqualTo("2s");
        assertThat(ConsulClientWrapper.format(Duration.ofSeconds(5))).isEqualTo("5s");
    }

    private static ConsulClientWrapper createWrapper() {
        ConsulClientFactory consulClientFactory = mock(ConsulClientFactory.class);
        TokenStorage tokenStorage = mock(TokenStorage.class);
        @SuppressWarnings("unchecked")
        Instance<TokenStorage> tokenStorageInstance = mock(Instance.class);
        doReturn(tokenStorage).when(tokenStorageInstance).get();
        return new ConsulClientWrapper(consulClientFactory, tokenStorageInstance);
    }
}
