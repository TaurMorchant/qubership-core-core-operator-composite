package com.netcracker.core.declarative.service.composite.consul.longpoll;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class PollSession {
    private final AtomicLong currentIndex = new AtomicLong(0);
    private final AtomicBoolean firstFired = new AtomicBoolean(false);

    public long currentIndex() {
        return currentIndex.get();
    }

    public boolean shouldEmit(long currentIndex, long newIndex, boolean fireOnFirstSuccess) {
        final boolean changed = newIndex > currentIndex;
        final boolean first = fireOnFirstSuccess && firstFired.compareAndSet(false, true);
        if (changed) {
            this.currentIndex.set(newIndex);
        }
        return changed || first;
    }
}