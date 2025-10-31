package com.netcracker.core.declarative.service.composite.consul.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CompositeStructureSerializer {
    private static final Pattern NAMESPACE_PATTERN = Pattern.compile("^composite/[^/]+/structure/([^/]+)/.*$");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static String serializeNamespacesPayload(ConsulPrefixSnapshot consulPrefixSnapshot) {
        List<String> namespaces = consulPrefixSnapshot.getKeySet().stream()
                .map(CompositeStructureSerializer::extractNamespace)
                .flatMap(Optional::stream)
                .distinct()
                .sorted()
                .toList();

        try {
            return OBJECT_MAPPER.writeValueAsString(Map.of("namespaces", namespaces));
        } catch (JsonProcessingException e) {
            throw new ConsulSnapshotSerializationException("Failed to serialize namespaces list", e);
        }
    }

    static Optional<String> extractNamespace(String key) {
        Matcher matcher = NAMESPACE_PATTERN.matcher(key);
        if (matcher.matches()) {
            return Optional.ofNullable(matcher.group(1));
        }
        return Optional.empty();
    }

    private CompositeStructureSerializer() {}
}
