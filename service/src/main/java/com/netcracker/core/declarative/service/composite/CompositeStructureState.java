package com.netcracker.core.declarative.service.composite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueList;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public record CompositeStructureState(Map<String, String> data) {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    public static CompositeStructureState from(KeyValueList list) {
        final List<KeyValue> entries = (list != null && list.getList() != null) ? list.getList() : Collections.emptyList();
        final Map<String, String> data = new TreeMap<>();
        for (KeyValue kv : entries) {
            data.put(kv.getKey(), kv.getValue());
        }
        return new CompositeStructureState(data);
    }

    public String toJson() throws JsonProcessingException {
        return MAPPER.writeValueAsString(data);
    }
}
