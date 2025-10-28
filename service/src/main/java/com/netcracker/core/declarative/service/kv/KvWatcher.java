package com.netcracker.core.declarative.service.kv;

import java.time.Duration;

public interface KvWatcher {
    void awaitChanges(String path, long index, Duration wait, KvHandler handler);
}
