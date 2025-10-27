package com.netcracker.core.declarative.service.composite;

import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Выделенная ответственность: извлечь prefix из значения ключа structureRef. */
public final class StructurePrefixResolver {

    public Optional<String> resolve(KeyValueList list, String structureRefKey) {
        Objects.requireNonNull(structureRefKey, "structureRefKey");
        if (list == null || list.getList() == null) return Optional.empty();
        List<KeyValue> all = list.getList();
        for (KeyValue kv : all) {
            if (structureRefKey.equals(kv.getKey())) {
                String v = kv.getValue();
                if (v != null && !v.isBlank()) return Optional.of(v.trim());
                return Optional.empty();
            }
        }
        return Optional.empty();
    }
}
