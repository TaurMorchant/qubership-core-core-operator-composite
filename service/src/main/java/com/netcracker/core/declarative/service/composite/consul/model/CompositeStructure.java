package com.netcracker.core.declarative.service.composite.consul.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record CompositeStructure(NamespaceRoles baseline, List<NamespaceRoles> satellites) {
}
