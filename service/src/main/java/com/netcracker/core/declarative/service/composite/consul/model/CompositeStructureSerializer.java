package com.netcracker.core.declarative.service.composite.consul.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CompositeStructureSerializer {
    private static final Pattern STRUCTURE_ENTRY_PATTERN = Pattern.compile("^composite/[^/]+/structure/([^/]+)/([^/]+)$");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static String serialize(ConsulPrefixSnapshot consulPrefixSnapshot) {
        LinkedHashMap<String, Namespace> namespaces = new LinkedHashMap<>();

        consulPrefixSnapshot.getKeySet().stream()
                .sorted()
                .forEach(key -> {
                    Matcher matcher = STRUCTURE_ENTRY_PATTERN.matcher(key);
                    if (!matcher.matches()) {
                        return;
                    }

                    String namespace = matcher.group(1);
                    String attribute = matcher.group(2);

                    Namespace namespaceEntry = namespaces.computeIfAbsent(namespace, Namespace::new);
                    String value = consulPrefixSnapshot.getValue(key);

                    switch (attribute) {
                        case "compositeRole" -> namespaceEntry.setCompositeRole(value);
                        case "bluegreenRole" -> namespaceEntry.setBlueGreenRole(value);
                        case "controllerNamespace" -> namespaceEntry.setControllerNamespace(value);
                        default -> {
                            //ignore not supported attributes
                        }
                    }
                });

        CompositeStructureSecretPayload payload = new CompositeStructureSecretPayload(
                buildBaseline(namespaces.values()),
                buildSatellites(namespaces.values())
        );

        try {
            return OBJECT_MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new ConsulSnapshotSerializationException("Failed to serialize composite structure", e);
        }
    }

    private static NamespaceRolesPayload buildBaseline(Collection<Namespace> namespaces) {
        List<Namespace> baselineNamespaces = namespaces.stream()
                .filter(entry -> entry.getCompositeRole() == CompositeRole.BASELINE)
                .toList();

        return buildNamespaceRoles(baselineNamespaces);
    }

    private static List<NamespaceRolesPayload> buildSatellites(Collection<Namespace> namespaces) {
        LinkedHashMap<String, List<Namespace>> satellites = new LinkedHashMap<>();

        namespaces.stream()
                .filter(entry -> entry.getCompositeRole() == CompositeRole.SATELLITE)
                .forEach(entry -> {
                    String satelliteKey = resolveSatelliteKey(entry);
                    satellites.computeIfAbsent(satelliteKey, k -> new LinkedList<>()).add(entry);
                });

        if (satellites.isEmpty()) {
            return Collections.emptyList();
        }

        return satellites.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .map(CompositeStructureSerializer::buildNamespaceRoles)
                .filter(Objects::nonNull)
                .toList();
    }

    private static String resolveSatelliteKey(Namespace entry) {
        if (entry.getBlueGreenRole() == BlueGreenRole.CONTROLLER) {
            return entry.getName();
        }
        String controllerNamespace = entry.getControllerNamespace();
        if (controllerNamespace != null && !controllerNamespace.isBlank()) {
            return controllerNamespace;
        }
        return entry.getName();
    }

    private static NamespaceRolesPayload buildNamespaceRoles(Collection<Namespace> namespaceEntries) {
        if (namespaceEntries.isEmpty()) {
            return null;
        }

        String controller = findNamespaceByBlueGreenRole(namespaceEntries, BlueGreenRole.CONTROLLER);
        String origin = findNamespaceByBlueGreenRole(namespaceEntries, BlueGreenRole.ORIGIN);
        String peer = findNamespaceByBlueGreenRole(namespaceEntries, BlueGreenRole.PEER);

        if (origin == null) {
            origin = namespaceEntries.stream()
                    .filter(entry -> entry.getBlueGreenRole() == null)
                    .map(Namespace::getName)
                    .sorted()
                    .findFirst()
                    .orElse(null);
        }

        if (controller == null && origin == null && peer == null) {
            return null;
        }

        return new NamespaceRolesPayload(controller, origin, peer);
    }

    private static String findNamespaceByBlueGreenRole(Collection<Namespace> namespaces, BlueGreenRole blueGreenRole) {
        return namespaces.stream()
                .filter(entry -> entry.getBlueGreenRole() == blueGreenRole)
                .map(Namespace::getName)
                .min(Comparator.naturalOrder())
                .orElse(null);
    }

    private enum CompositeRole {
        BASELINE,
        SATELLITE
    }

    private enum BlueGreenRole {
        CONTROLLER,
        ORIGIN,
        PEER
    }

    @Getter
    private static class Namespace {
        private final String name;
        private CompositeRole compositeRole;
        private BlueGreenRole blueGreenRole;
        @Setter
        private String controllerNamespace;

        Namespace(String name) {
            this.name = name;
        }

        void setCompositeRole(String value) {
            compositeRole = CompositeRole.valueOf(value.toUpperCase());
        }

        void setBlueGreenRole(String value) {
            blueGreenRole = BlueGreenRole.valueOf(value.toUpperCase());
        }
    }
}

