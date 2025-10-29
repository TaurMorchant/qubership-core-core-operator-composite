package com.netcracker.core.declarative.service.composite;

import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;

public final class StructurePrefixResolver {

    public String resolve(KeyValueList list, String structureRefKey) {
        if (list == null || list.getList() == null) {
            return null;
        }
        for (KeyValue kv : list.getList()) {
            if (structureRefKey.equals(kv.getKey())) {
                String value = kv.getValue();
                if (value != null && !value.isBlank()) {
                    return value.trim();
                }
                return null;
            }
        }
        return null;
    }
}
