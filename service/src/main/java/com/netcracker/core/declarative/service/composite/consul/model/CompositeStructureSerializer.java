package com.netcracker.core.declarative.service.composite.consul.model;

import lombok.Getter;
import lombok.Setter;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CompositeStructureSerializer {
    private static final Pattern STRUCTURE_ENTRY_PATTERN = Pattern.compile("^composite/[^/]+/structure/([^/]+)/([^/]+)$");

    public static CompositeStructure toPayload(ConsulPrefixSnapshot consulPrefixSnapshot) {
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

        return new CompositeStructure(
                buildBaseline(namespaces.values()),
                buildSatellites(namespaces.values())
        );
    }

    private static NamespaceRoles buildBaseline(Collection<Namespace> namespaces) {
        List<Namespace> baselineNamespaces = namespaces.stream()
                .filter(entry -> entry.getCompositeRole() == CompositeRole.BASELINE)
                .toList();

        return buildNamespaceRoles(baselineNamespaces);
    }

    private static List<NamespaceRoles> buildSatellites(Collection<Namespace> namespaces) {
        LinkedHashMap<String, List<Namespace>> satellites = new LinkedHashMap<>();

        namespaces.stream()
                .filter(entry -> entry.getCompositeRole() == CompositeRole.SATELLITE)
                .forEach(entry -> {
                    String satelliteKey = resolveSatelliteKey(entry);
                    satellites.computeIfAbsent(satelliteKey, k -> new ArrayList<>()).add(entry);
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

    private static NamespaceRoles buildNamespaceRoles(Collection<Namespace> namespaceEntries) {
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

        return new NamespaceRoles(controller, origin, peer);
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
            if (value == null || value.isBlank()) {
                return;
            }
            try {
                compositeRole = CompositeRole.valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new ConsulSnapshotSerializationException(
                        "Invalid composite role: " + value, e);
            }
        }

        void setBlueGreenRole(String value) {
            if (value == null || value.isBlank()) {
                return;
            }
            try {
                blueGreenRole = BlueGreenRole.valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new ConsulSnapshotSerializationException(
                        "Invalid blue-green role: " + value, e);
            }
        }
    }
}
