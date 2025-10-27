package com.netcracker.core.declarative.service.composite;

import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Строит полное состояние из ответа Consul. Без diff, без побочек. */
public final class StructureStateBuilder {

    public StructureState build(String prefix, KeyValueList list) {
        final List<KeyValue> entries = (list != null && list.getList() != null) ? list.getList() : List.of();
        final Map<String, String> data = new HashMap<>();
        for (KeyValue kv : entries) {
            data.put(kv.getKey(), kv.getValue());
        }
        return new StructureState(prefix, Map.copyOf(data));
    }
}
