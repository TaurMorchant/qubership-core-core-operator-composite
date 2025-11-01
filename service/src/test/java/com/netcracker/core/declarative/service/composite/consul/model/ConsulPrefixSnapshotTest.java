package com.netcracker.core.declarative.service.composite.consul.model;

import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class ConsulPrefixSnapshotTest {

    @Test
    void buildsSortedImmutableViewOfEntries() {
        KeyValue first = mock(KeyValue.class);
        when(first.getKey()).thenReturn("b");
        when(first.getValue()).thenReturn("value-b");

        KeyValue second = mock(KeyValue.class);
        when(second.getKey()).thenReturn("a");
        when(second.getValue()).thenReturn("value-a");

        KeyValue third = mock(KeyValue.class);
        when(third.getKey()).thenReturn("c");
        when(third.getValue()).thenReturn("value-c");

        KeyValueList keyValueList = mock(KeyValueList.class);
        when(keyValueList.getIndex()).thenReturn(123L);
        when(keyValueList.getList()).thenReturn(Arrays.asList(first, second, third));

        ConsulPrefixSnapshot snapshot = new ConsulPrefixSnapshot(keyValueList);

        assertThat(snapshot.getIndex()).isEqualTo(123L);
        assertThat(snapshot.getValue("a")).isEqualTo("value-a");
        assertThat(snapshot.getValue("b")).isEqualTo("value-b");
        assertThat(snapshot.getValue("c")).isEqualTo("value-c");

        Set<String> keys = snapshot.getKeySet();
        assertThat(keys).containsExactly("a", "b", "c");
        assertThatThrownBy(() -> keys.add("d")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void handlesNullListsGracefully() {
        ConsulPrefixSnapshot emptySnapshot = new ConsulPrefixSnapshot(null);

        assertThat(emptySnapshot.getIndex()).isZero();
        assertThat(emptySnapshot.getKeySet()).isEmpty();
        assertThat(emptySnapshot.getValue("missing")).isNull();

        KeyValueList keyValueList = mock(KeyValueList.class);
        when(keyValueList.getIndex()).thenReturn(55L);
        when(keyValueList.getList()).thenReturn(null);

        ConsulPrefixSnapshot snapshot = new ConsulPrefixSnapshot(keyValueList);

        assertThat(snapshot.getIndex()).isEqualTo(55L);
        assertThat(snapshot.getKeySet()).isEmpty();
        assertThat(snapshot.getValue("missing")).isNull();

        verify(keyValueList).getIndex();
        verify(keyValueList).getList();
        verifyNoMoreInteractions(keyValueList);
    }
}
