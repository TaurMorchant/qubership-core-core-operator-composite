package com.netcracker.core.declarative.service;

import io.vertx.ext.consul.ConsulClient;

public interface ConsulClientFactory {
        ConsulClient create(String token);
        ConsulClient create(String token, long timeout);
    }