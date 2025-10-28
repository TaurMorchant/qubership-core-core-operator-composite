package com.netcracker.core.declarative.service.kv;

import io.vertx.ext.consul.KeyValueList;

public interface KvHandler {
    void onSuccess(KeyValueList keyValueList);
    void onError(Throwable throwable);
}
