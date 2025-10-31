package com.netcracker.core.declarative.service.composite.consul.model;

import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;

import java.util.*;

public class ConsulPrefixSnapshot {

    private final KeyValueList keyValueList;
    private final Map<String, String> keyValueMap;

    public ConsulPrefixSnapshot(KeyValueList keyValueList) {
        this.keyValueList = keyValueList;
        this.keyValueMap = new TreeMap<>();

        final List<KeyValue> entries = (keyValueList != null && keyValueList.getList() != null) ?
                keyValueList.getList() :
                Collections.emptyList();
        for (KeyValue kv : entries) {
            keyValueMap.put(kv.getKey(), kv.getValue());
        }
    }

    public Set<String> getKeySet() {
        return Collections.unmodifiableSet(keyValueMap.keySet());
    }

    public long getIndex() {
        if (keyValueList == null) {
            return 0;
        }
        return keyValueList.getIndex();
    }

    public String getValue(String key) {
        return keyValueMap.get(key);
    }
}
