package com.netcracker.core.declarative.service.composite.consul.longpoll;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PollSessionTest {

    @Test
    void shouldEmitWhenIndexIncreasesAndUpdateCurrentIndex() {
        PollSession session = new PollSession();

        boolean emitted = session.shouldEmit(0L, 5L, false);

        assertThat(emitted).isTrue();
        assertThat(session.currentIndex()).isEqualTo(5L);
    }

    @Test
    void shouldEmitOnlyOnceOnFirstSuccessWhenIndexDoesNotChange() {
        PollSession session = new PollSession();

        boolean firstEmitted = session.shouldEmit(0L, 0L, true);
        boolean secondEmitted = session.shouldEmit(0L, 0L, true);

        assertThat(firstEmitted).isTrue();
        assertThat(secondEmitted).isFalse();
        assertThat(session.currentIndex()).isZero();
    }

    @Test
    void shouldNotEmitWhenIndexDoesNotIncreaseAndFirstSuccessDisabled() {
        PollSession session = new PollSession();

        session.shouldEmit(0L, 10L, false);
        boolean emitted = session.shouldEmit(10L, 10L, false);

        assertThat(emitted).isFalse();
        assertThat(session.currentIndex()).isEqualTo(10L);
    }
}
