package com.netcracker.core.declarative.service.composite.consul.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record NamespaceRolesPayload(String controller, String origin, String peer) {
}
