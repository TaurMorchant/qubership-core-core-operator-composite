package com.netcracker.core.declarative.service.composite.consul.longpoll;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks polling session state for Consul long-polling.
 * <p>
 * Determines when to emit snapshots based on Consul's modify index changes.
 * Thread-safe through atomic operations.
 */
public final class PollSession {
    private final AtomicLong currentIndex = new AtomicLong(0);
    private final AtomicBoolean firstFired = new AtomicBoolean(false);

    public long currentIndex() {
        return currentIndex.get();
    }

    /**
     * Resets the session state to allow re-processing of the current index.
     * Used when snapshot handling fails and retry is needed.
     */
    public void reset() {
        currentIndex.set(0);
        firstFired.set(false);
    }

    /**
     * Determines whether a snapshot should be emitted based on index changes.
     *
     * @param newIndex           the new modify index from Consul
     * @param fireOnFirstSuccess if true, emits on first successful poll regardless of index
     * @return true if the snapshot should be emitted to consumers
     */
    public boolean shouldEmit(long newIndex, boolean fireOnFirstSuccess) {
        long previous = currentIndex.getAndUpdate(current -> Math.max(newIndex, current));
        boolean changed = newIndex > previous;
        boolean first = fireOnFirstSuccess && firstFired.compareAndSet(false, true);
        return changed || first;
    }
}
