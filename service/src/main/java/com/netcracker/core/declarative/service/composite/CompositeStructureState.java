package com.netcracker.core.declarative.service.composite;

import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public record CompositeStructureState(Map<String, String> data) {
    public static CompositeStructureState from(KeyValueList list) {
        final List<KeyValue> entries = (list != null && list.getList() != null) ? list.getList() : Collections.emptyList();
        final Map<String, String> data = new HashMap<>();
        for (KeyValue kv : entries) {
            data.put(kv.getKey(), kv.getValue());
        }
        return new CompositeStructureState(data);
    }
}
