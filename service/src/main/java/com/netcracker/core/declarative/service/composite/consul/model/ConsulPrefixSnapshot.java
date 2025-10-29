package com.netcracker.core.declarative.service.composite.consul.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ConsulPrefixSnapshot {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    private final KeyValueList keyValueList;
    private final Map<String, String> keyValueMap;

    public ConsulPrefixSnapshot(KeyValueList keyValueList) {
        this.keyValueList = keyValueList;
        this.keyValueMap = new TreeMap<>();

        final List<KeyValue> entries = (keyValueList != null && keyValueList.getList() != null) ? keyValueList.getList() : Collections.emptyList();
        for (KeyValue kv : entries) {
            keyValueMap.put(kv.getKey(), kv.getValue());
        }
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

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(keyValueMap);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Unexpected exception during toJson transformation", e);
        }
    }
}
