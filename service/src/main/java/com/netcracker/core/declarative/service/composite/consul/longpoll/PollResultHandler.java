package com.netcracker.core.declarative.service.composite.consul.longpoll;

import com.netcracker.core.declarative.service.composite.consul.model.ConsulPrefixSnapshot;

public interface PollResultHandler {
    void onSuccess(ConsulPrefixSnapshot keyValueList);
    void onError(Throwable throwable);
}
