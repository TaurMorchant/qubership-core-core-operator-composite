package com.netcracker.core.declarative.service.composite.consul.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record CompositeStructureConfigMapPayload(
        String cloudProvider,
        String cloudOIDCProxyUrl,
        CompositeStructurePayload composite
) {
}
